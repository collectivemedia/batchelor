package com.collective.batchelor.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.eclipse.jetty.util.ConcurrentArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncBatchExecutor<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBatchExecutor.class);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    class Worker extends Thread {
        private final int batchSize;
        private volatile boolean running = true;
        private final List<T> messages;

        private Worker(int batchSize) {
            this.batchSize = batchSize;
            this.messages = Lists.newArrayListWithCapacity(batchSize);
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    // drain until we have a full batch or the duration is up
                    Queues.drain(queue,
                            messages,
                            batchSize,
                            queueDrainTime,
                            TimeUnit.MILLISECONDS);

                    if (queue.size() > batchSize) {
                        LOGGER.warn("queue size ({}) > batch size for {}", queue.size(), delegate.getClass().getSimpleName());
                    }

                    if (!messages.isEmpty()) {
                        // try again after one second until messages are processed
                        while (!delegate.handle(messages)) {
                            Thread.sleep(retryInterval);
                            if (queue.size() > batchSize) {
                                LOGGER.warn("queue size ({}) > batch size for {}", queue.size(), delegate.getClass().getSimpleName());
                            }
                        }
                        messages.clear();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void shutdown() {
            flushQueue();
            this.running = false;
        }

        private void flushQueue() {
            if (!this.running)
                return;

            // let the queue drain
            while (!queue.isEmpty() || !messages.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private final BatchHandler<T> delegate;
    private final BlockingQueue<T> queue;
    private final int queueSize;
    private final long queueDrainTime;
    private final long retryInterval;
    private volatile boolean append = false;

    final Worker workerThread;

    public AsyncBatchExecutor(final int batchSize, final int queueSize, BatchHandler<T> delegate, long queueDrainTime, long retryInterval) {
        this.queueSize = queueSize;
        this.delegate = delegate;
        this.queueDrainTime = queueDrainTime;
        this.queue = buildQueue();
        this.workerThread = new Worker(batchSize);
        this.retryInterval = retryInterval;
    }

    public static <T> AsyncBatchExecutor<T> createAndStart(final int batchSize, final int queueSize, BatchHandler<T> delegate, long queueDrainTime, long retryInterval) {
        AsyncBatchExecutor<T> asyncBatchExecutor = new AsyncBatchExecutor<>(batchSize, queueSize, delegate, queueDrainTime, retryInterval);
        asyncBatchExecutor.start();
        return asyncBatchExecutor;
    }

    private ConcurrentArrayBlockingQueue<T> buildQueue() {
        if (queueSize > 0) {
            return new ConcurrentArrayBlockingQueue.Bounded<>(queueSize);
        }
        return new ConcurrentArrayBlockingQueue.Unbounded<>();
    }

    public int getMaxQueueSize() {
        return queueSize;
    }

    public int getQueueSize() {
        return queue.size();
    }

    public float getQueueFillFraction() {
        // unbounded
        if (queueSize == 0)
            return 0;

        return (float) queue.size() / (float) queueSize;
    }

    public void start() {
        workerThread.setName("asyncBatchWorkerThread-" + THREAD_COUNTER.incrementAndGet());
        workerThread.setPriority(Thread.MIN_PRIORITY);
        workerThread.start();
        append = true;
    }

    public void stop() {
        append = false;
        workerThread.shutdown();
        delegate.done();
    }

    public void flush() {
        workerThread.flushQueue();
    }

    public void add(T event) {
        if (!append)
            return;
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            LOGGER.error("put failed", e);
        }
    }
}
