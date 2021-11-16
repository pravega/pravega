package io.pravega.shared.health.impl;

import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NullHealthContributor extends AbstractHealthContributor {

    public static final NullHealthContributor INSTANCE = new NullHealthContributor();

    NullHealthContributor() {
        super("null");
    }


    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        log.error("Improperly connected HealthContributor.");
        return Status.UNKNOWN;
    }
}
