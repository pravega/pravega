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
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthDaemon;
import io.pravega.shared.health.HealthService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class HealthDaemonImpl implements HealthDaemon {

    /**
     * The {@link Health} the daemon should report in the case it has not started querying the {@link HealthService}.
     */
    private static final Health INITIAL_HEALTH = Health.builder().build();

    /**
     * Represents the most recent {@link Health} information provided by the {@link ScheduledExecutorService}.
     */
    AtomicReference<Health> latest = new AtomicReference<Health>(INITIAL_HEALTH);

    /**
     * The {@link HealthService} associated with this {@link HealthDaemon}.
     */
    private final HealthService service;

    /**
     * The underlying {@link ScheduledExecutorService} used to executor the recurring service-level {@link Health} check.
     */
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "health-daemon");

    /**
     * The interval at which to run the health check.
     */
    @Getter
    @Setter
    private int interval = DEFAULT_INTERVAL_SECONDS;

    /**
     * The {@link Future} representing the ongoing health check.
     */
    private Future<?> future;

    /**
     * Starts the underlying {@link ScheduledExecutorService} to repeatedly call {@link io.pravega.shared.health.HealthEndpoint#health(boolean)}.
     */
    public void start() {
        log.info("Starting a HealthDaemon thread -- running at {} second intervals.", interval);
        future = executor.scheduleAtFixedRate(() -> {
                    latest.set(service.endpoint().health(true));
                },
                interval,
                interval,
                TimeUnit.SECONDS);
    }

    /**
     * Stops any ongoing health checks. May be restarted by calling {@link HealthDaemon#start()}.
     */
    public void stop() {
        log.info("Cancelling the active HealthDaemon thread.");
        future.cancel(true);
        future = null;
    }

    /**
     * Permanently shuts down the {@link HealthDaemon}'s {@link ScheduledExecutorService}. It will not be able to be restarted.
     */
    public void shutdown() {
        log.info("Permanently shutting down the HealthDaemon.");
        executor.shutdownNow();
        try {
            executor.awaitTermination(interval, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Unexpected InterruptedException shutting down HealthDaemon.", e);
        }
        future = null;
        // The HealthDaemon should be able to continue to report a healthy result.
        latest.set(INITIAL_HEALTH);
    }

    /**
     * Determines if the {@link HealthDaemon} is actively performing recurring health checks.
     * @return The {@link HealthDaemon} running status.
     */
    public boolean isRunning() {
        if (future == null || (executor.isTerminated() && executor.isShutdown())) {
            return false;
        }
        return true;
    }

    /**
     * Returns the {@link HealthDaemon} to its state had it just been initialized.
     */
    public void reset() {
        stop();
        latest.set(INITIAL_HEALTH);
    }

    /**
     * Provides the latest {@link Health} result of the recurring {@link io.pravega.shared.health.HealthEndpoint#health(boolean)} calls.
     * @return The latest {@link Health} result.
     */
    public Health getLatestHealth() {
        return latest.get();
    }
}
