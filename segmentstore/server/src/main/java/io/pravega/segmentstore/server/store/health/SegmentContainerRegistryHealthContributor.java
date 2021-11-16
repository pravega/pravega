package io.pravega.segmentstore.server.store.health;

import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;

public class SegmentContainerRegistryHealthContributor extends AbstractHealthContributor {
    private final SegmentContainerRegistry segmentContainerRegistry;

    public SegmentContainerRegistryHealthContributor(@NonNull SegmentContainerRegistry segmentContainerRegistry) {
        super("SegmentContainerRegistry");
        this.segmentContainerRegistry = segmentContainerRegistry;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {

        Status status = Status.TERMINATED;
        boolean ready = !segmentContainerRegistry.isClosed();

        if (ready) {
            status = Status.RUNNING;
        }

        return status;
    }
}
