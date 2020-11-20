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
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class HealthDaemonImpl implements HealthDaemon {

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
    private final int interval;

    /**
     * The {@link Future} representing the ongoing health check.
     */
    private Future<?> future;

    public void start() {
        log.info("Starting a HealthDaemon thread -- running at {} second intervals.", interval);
        future = executor.scheduleAtFixedRate(() -> {
                    latest.set(service.endpoint().health(true));
                },
                interval,
                interval,
                TimeUnit.SECONDS);
    }

    public void stop() {
        log.info("Cancelling active HealthDaemon thread.");
        future.cancel(true);
        future = null;
    }

    public void shutdown() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(interval, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Unexpected InterruptedException shutting down HealthDaemon.", e);
        }
    }

    public boolean isRunning() {
        if (future == null || (executor.isTerminated() && executor.isShutdown())) {
            return false;
        }
        return true;
    }

    public void reset() {
        stop();
        latest.set(INITIAL_HEALTH);
    }

    public Health getLatestHealth() {
        if (latest.get() == null) {
            return INITIAL_HEALTH;
        }
        return latest.get();
    }
}
