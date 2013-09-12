package com.collective.batchelor.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;


public class AsyncBatchExecutorTest {

    private AsyncBatchExecutor<String> executorService;
    private BatchHandler<String> handler;

    @Before
    public void setupService() {
        //noinspection unchecked
        handler = mock(BatchHandler.class, BatchHandler.class.getName());
        executorService = new AsyncBatchExecutor<>(1, 1, handler, 500, 1000);
        assertThat(executorService.getMaxQueueSize()).isEqualTo(1);
        assertThat(executorService.getQueueSize()).isEqualTo(0);
    }

    @After
    public void stopService() {
        executorService.stop();
        verify(handler, atLeastOnce()).done();
    }

    @Test
    public void canReturnQueueFillPercentage() throws InterruptedException {
        executorService = new AsyncBatchExecutor<>(1, 3, handler, 500, 1000);
        executorService.start();
        executorService.add("bla");
        // wait till the worker thread grabs the first element
        while (executorService.getQueueSize() > 0)
            Thread.sleep(10);

        executorService.add("bla");
        assertThat(executorService.getQueueFillFraction()).isEqualTo(1f / 3f);
        executorService.add("bla");
        assertThat(executorService.getQueueFillFraction()).isEqualTo(2f / 3f);
        executorService.add("bla");
        assertThat(executorService.getQueueFillFraction()).isEqualTo(1f);

        when(handler.handle(anyListOf(String.class))).thenReturn(true);
    }

    @Test
    public void willDispatchToHandler() {
        executorService.start();
        assertThat(executorService.workerThread.getState()).isEqualTo(Thread.State.RUNNABLE);
        when(handler.handle(anyListOf(String.class))).thenReturn(true);
        executorService.add("bla");
        verify(handler, timeout(1000)).handle(anyListOf(String.class));
    }

    @Test
    public void canBeCreatedWithoutMetrics() {
        executorService = new AsyncBatchExecutor<>(1, 1, handler, 500, 1000);
        executorService.start();
        assertThat(executorService.workerThread.getState()).isEqualTo(Thread.State.RUNNABLE);
        when(handler.handle(anyListOf(String.class))).thenReturn(true);
        executorService.add("bla");
        verify(handler, timeout(1000)).handle(anyListOf(String.class));
    }

    @Test
    public void wontFlushUntilHandlerSaysThatEventsAreProcessed() throws InterruptedException {
        executorService.start();
        when(handler.handle(anyListOf(String.class))).thenReturn(false);
        executorService.add("bla");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                executorService.flush();
                latch.countDown();
            }
        }).start();
        assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isFalse();
        when(handler.handle(anyListOf(String.class))).thenReturn(true);
        assertThat(latch.await(2000, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    public void wontAddToQueueIfNotStarted() {
        assertThat(executorService.workerThread.getState()).isEqualTo(Thread.State.NEW);
        assertThat(executorService.getQueueSize()).isEqualTo(0);
        executorService.add("bla");
        assertThat(executorService.getQueueSize()).isEqualTo(0);
    }

    @Test
    public void canFlushNotStartedExecutorService() {
        executorService.flush();
    }

    @Test
    public void canStopNotStartedExecutorService() {
        executorService.stop();
    }

    @Test
    public void blocksIfBoundedQueueIsUsedAndQueueIsFull() throws InterruptedException {
        executorService.stop();
        when(handler.handle(anyListOf(String.class))).thenReturn(false);
        executorService = AsyncBatchExecutor.createAndStart(1, 1, handler, 500, 1000);
        // first entry will be fetched into the list and removed from the queue
        executorService.add("bla");
        // second entry fills the queue
        executorService.add("bla1");

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                executorService.add("bla2");
                latch.countDown();
            }
        }).start();
        // third entry can't be added
        assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isFalse();
        when(handler.handle(anyListOf(String.class))).thenReturn(true);
        assertThat(latch.await(2000, TimeUnit.MILLISECONDS)).isTrue();
        verify(handler, atLeast(3)).handle(anyListOf(String.class));
    }

}
