package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.common.hash.ConsistentHash;
import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

public class AdminImpl implements Api.Admin {
    // TODO: read from configuration
    private final int NUM_OF_CONTAINERS = 64;
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    public AdminImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    /***
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously notify all pravega hosts about segments in the stream
     */
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        String stream = streamConfig.getName();
        return CompletableFuture.supplyAsync(() -> streamStore.createStream(stream, streamConfig))
                .thenApply(result -> {
                    if(result) {
                        for (int i = 0; i < streamConfig.getScalingingPolicy().getMinNumSegments(); i++) {
                            // create segments, compute their hashes
                            // TODO: Figure out how to define key ranges
                            Segment segment = new Segment(i, 0, Long.MAX_VALUE, 0.0, 0.0);
                            streamStore.addActiveSegment(stream, segment);
                            int container = ConsistentHash.hash(stream + segment.getNumber(), NUM_OF_CONTAINERS);
                            Host host = hostStore.getHostForContainer(container);
                            // TODO: make asynchronous and non blocking call into host to inform it about new segment ownership.
                        }
                        return Status.SUCCESS;
                    }
                    else return Status.FAILURE;
                });
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        throw new NotImplementedException();
    }
}
