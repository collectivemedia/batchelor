package com.collective.batchelor.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.*;

public class FlumeBatchHandlerTest {

    private FlumeTestServer flumeTestServer;

    @Before
    public void startFlume() {
        flumeTestServer = new FlumeTestServer(false, "127.0.0.1", 49151);
    }

    @After
    public void stopFlume() throws InterruptedException {
        flumeTestServer.close();
    }

    @Test
    public void canProcessEvents() throws InterruptedException {
        FlumeBatchHandler flumeBatchHandler = new FlumeBatchHandler("127.0.0.1", 49151, 20000);
        assertThat(flumeBatchHandler.handle(Arrays.asList(createFlumeEvent("hallo")))).isTrue();
        flumeBatchHandler.done();
        Event event = flumeTestServer.pollFlumeEvent(200);
        assertThat(event.getBody()).isEqualTo("hallo".getBytes());
    }

    @Test
    public void resetsRpcClientAndReturnsFalseWhenEventCantBeSend() throws EventDeliveryException {
        FlumeBatchHandler flumeBatchHandler = spy(new FlumeBatchHandler("127.0.0.1", 49151, 20000));
        RpcClient rpcClient = mock(RpcClient.class);
        Mockito.doThrow(EventDeliveryException.class).when(rpcClient).appendBatch(anyListOf(Event.class));
        when(flumeBatchHandler.getRpcClient()).thenReturn(rpcClient);
        assertThat(flumeBatchHandler.handle(Arrays.asList(createFlumeEvent("hallo")))).isFalse();
        verify(flumeBatchHandler).resetRpcClient();
        assertThat(flumeBatchHandler.rpcClient).isNull();
    }

    private org.apache.flume.Event createFlumeEvent(String body) {
        byte[] bytes = body.getBytes();
        return EventBuilder.withBody(bytes);
    }


}
