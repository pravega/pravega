package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.stream.Api;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.stream.StreamSegments;
import org.apache.commons.lang.NotImplementedException;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ProducerImpl implements Api.Producer{
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    public ProducerImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
        // fetch active segments from segment store
        return CompletableFuture.supplyAsync(() -> streamStore.getActiveSegments(stream))
                .thenApply(
                        result -> new StreamSegments(result.getCurrent().stream()
                        .map(x -> SegmentHelper.getSegmentId(stream, streamStore.getSegment(stream, x.intValue()), hostStore))
                        .collect(Collectors.toList()), System.currentTimeMillis()));
    }

    @Override
    public CompletableFuture<URI> getURI(SegmentId id) {
        throw new NotImplementedException();
    }
}
