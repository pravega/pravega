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

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.HealthDaemon;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

@Slf4j
public class HealthServiceImpl implements HealthService {

    private ContributorRegistryImpl registry;

    /**
     * The {@link HealthConfig} object used to setup the {@link HealthComponent} hierarchy.
     */
    private HealthConfig config;

    /**
     * The {@link HealthDaemon} used *if* a daemon is passed in during construction.
     */
    private final HealthDaemon daemon;

    private final HealthEndpoint endpoint;

    public HealthServiceImpl(HealthConfig config) {
        this.config = config;
        this.registry = new ContributorRegistryImpl();
        this.endpoint = new HealthEndpointImpl(this.registry);
        this.config.reconcile(this.registry);
        // Risk of escaped 'this' is we call HealthDaemon.start() within this (HealthServiceImpl) constructor?
        this.daemon = new HealthDaemonImpl(this);
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
    public HealthDaemon daemon() {
        return daemon;
    }

    @Override
    public void clear() {
        this.registry.reset();
    }
}
