package io.pravega.segmentstore.server.store.health;

import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;

public class StreamSegmentContainerHealthContributor extends AbstractHealthContributor {

    private final SegmentContainer container;

    public StreamSegmentContainerHealthContributor(String name, SegmentContainer container) {
        super(name);
        this.container = container;
    }
    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        return null;
    }
}
