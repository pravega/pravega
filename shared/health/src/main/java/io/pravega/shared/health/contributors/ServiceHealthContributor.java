package io.pravega.shared.health.contributors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServiceHealthContributor extends AbstractHealthContributor {

    private final Service service;

    public ServiceHealthContributor(String name, Service service) {
        super(name);
        this.service = service;
    }

    // Maps the {@link Service.State} of the AbstractService to the corresponding health {@link Status}.
    private Status map(Service.State state) {
        Status status = Status.valueOf(state.name());
        if (status == null) {
            log.error("Unexpected Service State: {}, no corresponding Health Status.", state.name());
            return Status.UNKNOWN;
        }
        return status;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        builder.details(ImmutableMap.of("State", service.state()));
        return map(service.state());
    }
}
