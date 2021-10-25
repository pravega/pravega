/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.health.impl;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthServiceUpdater;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class HealthServiceUpdaterImpl extends AbstractScheduledService implements HealthServiceUpdater {

    /**
     * The {@link Health} the daemon should report in the case it has not started querying the {@link io.pravega.shared.health.HealthServiceManager}.
     */
    private static final Health INITIAL_HEALTH = Health.builder().build();

    /**
     * Represents the most recent {@link Health} information provided by the {@link ScheduledExecutorService}.
     */
    private final AtomicReference<Health> latest = new AtomicReference<Health>(INITIAL_HEALTH);

    /**
     * The {@link io.pravega.shared.health.HealthServiceManager} associated with this {@link HealthServiceUpdater}.
     */
    private final HealthContributor root;

    /**
     * The interval at which to run the health check.
     */
    @Getter
    private final Duration interval;

    /**
     * The underlying {@link ScheduledExecutorService} used to executor the recurring service-level {@link Health} check.
     */
    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "health-service-updater", Thread.MIN_PRIORITY);

    /**
     * Provides the latest {@link Health} result of the recurring {@link io.pravega.shared.health.HealthEndpoint#getHealth()} calls.
     * @return The latest {@link Health} result.
     */
    @Override
    public Health getLatestHealth() {
        return latest.get();
    }

    @Override
    protected ScheduledExecutorService executor() {
        return this.executorService;
    }

    @Override
    protected void runOneIteration() {
        latest.set(root.getHealthSnapshot());
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(interval, interval);
    }

    /**
     * Starts the underlying {@link ScheduledExecutorService} to repeatedly call {@link io.pravega.shared.health.HealthEndpoint#getHealth()}.
     */
    @Override
    protected void startUp() {
        log.info("Starting the HealthServiceUpdater, running at {} intervals.", interval);
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
     * A {@link HealthServiceUpdater} is only reachable though a {@link io.pravega.shared.health.HealthServiceManager}.
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
        ExecutorServiceHelpers.shutdown(Duration.ofSeconds(5), executorService);
    }
}
