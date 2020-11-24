/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
public class InlineHealthIndicator extends HealthIndicator {

    private final Function<Health.HealthBuilder, Void> doHealthCheck;

    InlineHealthIndicator(String name, Function<Health.HealthBuilder, Void> doHealthCheck) {
        this(name, doHealthCheck, new DetailsProvider());
    }

    InlineHealthIndicator(String name, Function<Health.HealthBuilder, Void> doHealthCheck, DetailsProvider provider) {
        super(name, provider);
        this.doHealthCheck = doHealthCheck;
    }

    @Override
    public Health health(boolean includeDetails) {
        Health.HealthBuilder builder = new Health.HealthBuilder();
        try {
            doHealthCheck.apply(builder);
        } catch (Exception ex) {
            log.warn(this.healthCheckFailedMessage());
            builder.status(Status.DOWN);
        }
        if (includeDetails) {
            builder.details(provider.fetch());
        }
        return builder.name(getName()).build();
    }

    @Override
    public void doHealthCheck(Health.HealthBuilder builder) throws Exception {
        log.error("NOP -- an InlineHealthIndicator should supply health check logic via a lambda.");
    }
}
