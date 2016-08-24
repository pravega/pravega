package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.StreamSegments;
import org.apache.commons.lang.NotImplementedException;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProducerImpl implements Api.Producer{
    @Override
    public CompletableFuture<List<StreamSegments>> getCurrentSegments(String stream) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<URI> getURI(SegmentId id) {
        throw new NotImplementedException();
    }
}
