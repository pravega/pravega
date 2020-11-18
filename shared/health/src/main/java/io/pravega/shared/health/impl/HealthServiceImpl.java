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
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthComponent;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * The {@link ScheduledExecutorService} used for recurring health checks.
     */
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "health-check");

    private final HealthEndpoint endpoint;

    private final HealthDaemon daemon;

    public HealthServiceImpl(HealthConfig config) {
        this.registry = new ContributorRegistryImpl();
        this.config = config;
        this.config.reconcile(this.registry);
        this.endpoint = new HealthEndpointImpl(this.registry);
        this.daemon = new HealthDaemon(this, executor, HEALTH_MONITOR_INTERVAL_SECONDS);
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
        this.daemon.clear();
        this.registry.clear();
    }

    @RequiredArgsConstructor
    public class HealthDaemon {
        /**
         * Represents the most recent {@link Health} information provided by the {@link ScheduledExecutorService}.
         */
        AtomicReference<Health> latest = new AtomicReference<Health>();

        /**
         * The {@link HealthService} associated with this {@link HealthDaemon}.
         */
        private final HealthService service;

        /**
         * The underlying {@link ScheduledExecutorService} used to executor the recurring service-level {@link Health} check.
         */
        private final ScheduledExecutorService executor;

        /**
         * The interval at which to run the health check.
         */
        private final int interval;

        public void start() {
            log.info("Starting HealthDaemon service -- running at {} second intervals.", interval);
            executor.scheduleAtFixedRate(() -> latest.set(service.endpoint().health(true)),
                    interval,
                    interval,
                    TimeUnit.SECONDS);
        }

        public void stop() {
            executor.shutdownNow();
            try {
                executor.awaitTermination(HEALTH_MONITOR_INTERVAL_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Unexpected InterruptedException shutting down HealthDaemon.", e);
            }
        }

        public boolean running() {
            if (!executor.isTerminated() && !executor.isShutdown()) {
                return true;
            }
            return false;
        }

        public void clear() {
            stop();
            latest.set(null);
        }
    }
}
