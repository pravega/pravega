package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.model.Position;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ConsumerImpl implements Api.Consumer{
    @Override
    public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int count) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<List<Position>> updatePositions(List<Position> positions) {
        throw new NotImplementedException();
    }
}
