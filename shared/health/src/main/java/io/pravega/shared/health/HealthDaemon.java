/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;


/**
 * The {@link HealthDaemon} is responsible for regularly updating the {@link Health} of the {@link HealthService}.
 * This is useful in cases where health information is not regularly queried by some client. In the event of a crash or failure,
 * (should we want to provide this information) it allows us to place an upper bound on how stale this {@link Health}
 * information may be.
 */
public interface HealthDaemon {

    public static final int DEFAULT_INTERVAL_SECONDS = 10;
    /**
     * Supplies the most recent {@link Health} check result.
     *
     * @return The {@link Health} of the last health check.
     */
    Health getLatestHealth();

    /**
     * Starts the {@link HealthDaemon} thread (not to be confused with a 'daemon thread') which checks the health.
     */
    void start();

    /**
     * Stop the {@link HealthDaemon} thread.
     */
    void stop();

    /**
     * Shutdown the {@link HealthDaemon} -- rendering it unable to perform future health checks.
     */
    void shutdown();

    /**
     * Resets the {@link HealthDaemon} thread, returning it to its default state.
     */
    void reset();

    /**
     * Checks to see if the underlying {@link HealthDaemon} thread is actively running.
     */
    boolean isRunning();

    /**
     * The interval (in seconds) at which the {@link HealthDaemon} performs the health checks in.
     * @return
     */
    int getInterval();

}
