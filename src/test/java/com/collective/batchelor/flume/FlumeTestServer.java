package com.collective.batchelor.flume;

import com.google.common.base.Preconditions;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class FlumeTestServer implements AvroSourceProtocol {
    private final NettyServer flumeServer;
    private final boolean ignoreData;

    public FlumeTestServer(boolean ignoreData, String host, int port) throws ChannelException {
        this.ignoreData = ignoreData;
        Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);
        try {
            flumeServer = new NettyServer(responder, new InetSocketAddress(host, port));
            flumeServer.start();
        } catch (ChannelException e) {
            // debug the ports
            ProcessBuilder pb = new ProcessBuilder("lsof", "-iTCP:" + port, "-sTCP:LISTEN");
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            try {
                Process p = pb.start();
                p.waitFor();
            } catch (IOException | InterruptedException ex) {
                ex.printStackTrace();
            }
            throw e;
        }
    }

    public final Queue<AvroFlumeEvent> events = new LinkedBlockingQueue<>();

    public Event pollFlumeEvent(int retries) throws InterruptedException {
        Event event = null;
        while (retries-- > 0 && ((event = poll()) == null)) {
            Thread.sleep(10);
        }
        return event;
    }


    public Event poll() {
        AvroFlumeEvent avroEvent = events.poll();
        if (avroEvent != null) {
            return EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));
        }
        return null;
    }

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
        if (!ignoreData)
            events.add(event);
        return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
        if (!ignoreData)
            Preconditions.checkState(this.events.addAll(events));
        return Status.OK;
    }

    private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
        Map<String, String> stringMap = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return stringMap;
    }

    public void close() throws InterruptedException {
        flumeServer.close();
        flumeServer.join();
    }
}
