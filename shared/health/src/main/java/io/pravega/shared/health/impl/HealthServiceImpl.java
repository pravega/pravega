/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class HealthServiceImpl implements HealthService {

    private final static int HEALTH_MONITOR_INTERVAL_SECONDS = 10;
    /**
     * The interval at which the {@link ScheduledExecutorService} will query the health of the root {@link HealthComponent}.
     */
    private ContributorRegistryImpl registry;

    /**
     * The {@link HealthConfig} object used to setup the {@link HealthComponent} hierarchy.
     */
    private HealthConfig config;


    private final HealthEndpoint endpoint;

    public HealthServiceImpl(HealthConfig config) {
        this.registry = new ContributorRegistryImpl();
        this.config = config;
        this.config.reconcile(this.registry);
        this.endpoint = new HealthEndpointImpl(this.registry);
    }

    @Override
    public Collection<String> components() {
        return registry().components();
    }

    @Override
    public ContributorRegistry registry() {
        return this.registry;
    }

    @Override
    public HealthEndpoint endpoint() {
        return this.endpoint;
    }

    @Override
    public void clear() {
        this.registry.reset();
    }
}
