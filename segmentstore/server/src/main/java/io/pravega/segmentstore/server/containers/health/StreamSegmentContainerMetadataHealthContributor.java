package io.pravega.segmentstore.server.containers.health;

import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;

public class StreamSegmentContainerMetadataHealthContributor extends AbstractHealthContributor {

    private final StreamSegmentContainerMetadata metadata;

    public StreamSegmentContainerMetadataHealthContributor(@NonNull String name, StreamSegmentContainerMetadata metadata) {
        super(name);
        this.metadata = metadata;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {

        builder.details(ImmutableMap.of(
                "ActiveSegmentCount", metadata.getActiveSegmentCount(),
                "OperationSequenceNumber", metadata.getOperationSequenceNumber(),
                "LastTruncatedSequenceNumber", metadata.getLastTruncatedSequenceNumber()
        ));

        return Status.RUNNING;
    }
}
