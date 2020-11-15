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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthComponent;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthService;
import io.pravega.shared.health.StatusAggregator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collection;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HealthServiceImpl implements HealthService {

    /**
     * The interval at which the {@link ScheduledExecutorService} will query the health of the root {@link HealthComponent}.
     */
    @VisibleForTesting
    public ContributorRegistryImpl registry;

    private HealthConfig config;

    private Stack<HealthComponent> components;

    /**
     * The {@link ScheduledExecutorService} used for recurring health checks.
     */
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "health-check");

    public HealthServiceImpl(HealthConfig config) {
        executor.scheduleAtFixedRate(() -> health(true),
                HEALTH_MONITOR_INTERVAL_SECONDS,
                HEALTH_MONITOR_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
        registry = new ContributorRegistryImpl();
        initialize(registry, config);
    }

    public void initialize(ContributorRegistry registry, HealthConfig config, String current) {
        for (val contributor : config.relations().entrySet())  {

        }
    }

    private void register(HealthConfig config, String current, String parent) {
        StatusAggregator aggregator = config.aggregators().get(current);
        // Fetch the parent component -- which should exist at this time.
        Optional<HealthContributor> contributor = registry.get(parent);
        // Register the component as described by the HealthConfig.
        components.push(new HealthComponent(current, aggregator, registry));
        registry.register(components.peek(), contributor.get());
        // Register all dependencies.
        for (String name : config.relations().get(current)) {
            register(config, name, current);
        }
    }

    public Health health(String name, boolean includeDetails) {
        Optional<HealthContributor> result = registry.get(name);
        if (result.isEmpty()) {
            log.error("No HealthComponent with name: {} found in the registry.", name);
            return null;
        }
        return result.get().health(includeDetails);
    }

    public Health health(boolean includeDetails) {
        return health(registry.root(), includeDetails);
    }

    public ContributorRegistry registry() {
        return this.registry;
    }

    public Collection<HealthComponent> components() {
        return this.components;
    }

}
