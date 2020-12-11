/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthServiceUpdater;
import io.pravega.shared.health.HealthService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class HealthServiceUpdaterImpl extends AbstractScheduledService implements HealthServiceUpdater {

    /**
     * The {@link Health} the daemon should report in the case it has not started querying the {@link HealthService}.
     */
    private static final Health INITIAL_HEALTH = Health.builder().build();

    /**
     * Represents the most recent {@link Health} information provided by the {@link ScheduledExecutorService}.
     */
    private final AtomicReference<Health> latest = new AtomicReference<Health>(INITIAL_HEALTH);

    /**
     * The {@link HealthService} associated with this {@link HealthServiceUpdater}.
     */
    private final HealthService service;

    /**
     * The underlying {@link ScheduledExecutorService} used to executor the recurring service-level {@link Health} check.
     */
    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "health-service-updater", Thread.MIN_PRIORITY);

    /**
     * The interval at which to run the health check.
     */
    @Getter
    @Setter
    private int interval = DEFAULT_INTERVAL_SECONDS;

    /**
     * Provides the latest {@link Health} result of the recurring {@link io.pravega.shared.health.HealthEndpoint#getHealth(boolean)} calls.
     * @return The latest {@link Health} result.
     */
    public Health getLatestHealth() {
        return latest.get();
    }

    @Override
    protected ScheduledExecutorService executor() {
        return this.executorService;
    }

    @Override
    protected void runOneIteration() {
        latest.set(service.endpoint().getHealth(true));
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(Duration.ofSeconds(interval), Duration.ofSeconds(interval));
    }

    /**
     * Starts the underlying {@link ScheduledExecutorService} to repeatedly call {@link io.pravega.shared.health.HealthEndpoint#getHealth(boolean)}.
     */
    @Override
    protected void startUp() {
        log.info("Starting the HealthServiceUpdater, running at {} SECOND intervals.", interval);
    }

    /**
     * Permanently shuts down the {@link HealthServiceUpdater}'s {@link ScheduledExecutorService}. It will not be able to be restarted.
     *
     */
    @Override
    protected void shutDown() {
        log.info("Shutting down the HealthServiceUpdater.");
        latest.set(INITIAL_HEALTH);
    }

    /**
     * A {@link HealthServiceUpdater} is only reachable though a {@link HealthService}.
     */
    @Override
    public void close() {
        if (isRunning()) {
            shutDown();
        }
        if (state() == State.RUNNING) {
            Futures.await(Services.stopAsync(this, this.executorService));
        }
        log.info("Stopping ScheduledExecutorService.");
        try {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Error closing down ScheduledExecutorService.", e);
        }
    }
}
