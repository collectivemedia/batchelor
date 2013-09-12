package com.collective.batchelor.flume;


import com.collective.batchelor.util.BatchHandler;
import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlumeBatchHandler implements BatchHandler<Event> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlumeBatchHandler.class);
    RpcClient rpcClient = null;

    private final int batchSize;
    private final String host;
    private final Integer port;

    public FlumeBatchHandler(String host, Integer port, final int batchSize) {
        this.batchSize = batchSize;
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean handle(List<Event> batch) {
        try {
            getRpcClient().appendBatch(batch);
        } catch (Exception e) {
            LOGGER.warn("send batch failed - wait for next run", e);
            resetRpcClient();
            return false;
        }
        return true;
    }

    @Override
    public void done() {
        if (rpcClient != null) {
            rpcClient.close();
        }
    }


    void resetRpcClient() {
        if (rpcClient != null) {
            rpcClient.close();
            rpcClient = null;
        }
    }

    RpcClient getRpcClient() {
        if (rpcClient != null && !rpcClient.isActive()) {
            resetRpcClient();
        }
        if (rpcClient == null) {
            rpcClient = RpcClientFactory.getDefaultInstance(host, port, batchSize);
        }
        return rpcClient;
    }
}
