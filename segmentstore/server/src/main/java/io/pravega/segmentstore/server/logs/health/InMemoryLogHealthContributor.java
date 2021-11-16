package io.pravega.segmentstore.server.logs.health;

import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.server.logs.InMemoryLog;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;

public class InMemoryLogHealthContributor extends AbstractHealthContributor {

    private final InMemoryLog log;

    public InMemoryLogHealthContributor(@NonNull String name, InMemoryLog log) {
        super(name);
        this.log = log;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        builder.details(ImmutableMap.of("Size", log.size()));
        return Status.RUNNING;
    }
}
