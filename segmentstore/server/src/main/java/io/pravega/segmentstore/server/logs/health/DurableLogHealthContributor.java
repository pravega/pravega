package io.pravega.segmentstore.server.logs.health;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import io.pravega.segmentstore.server.logs.DurableLog;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.contributors.ServiceHealthContributor;

public class DurableLogHealthContributor extends ServiceHealthContributor {

    private final DurableLog log;

    public DurableLogHealthContributor(String name, DurableLog log) {
        super(name, log);
        this.log = log;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        Status status = super.doHealthCheck(builder);

        if (log.isOffline()) {
            status = Status.TERMINATED;
        }

        builder.details(ImmutableMap.of("State", log.state(), "IsOffline", log.isOffline()));

        return status;
    }

}
