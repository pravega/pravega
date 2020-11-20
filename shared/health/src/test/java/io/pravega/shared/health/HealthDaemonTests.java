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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.SampleFailingIndicator;

import io.pravega.test.common.AssertExtensions;

@Slf4j
public class HealthDaemonTests {

    HealthService service;

    HealthDaemon daemon;

    @BeforeClass
    public static void initialize() {
        HealthProvider.initialize();
    }

    @Before
    public void before() {
        service = HealthProvider.getHealthService();
        daemon = HealthProvider.getHealthDaemon();
    }

    @Test
    public void isRunningAfterServiceInitialization() {
        Assert.assertTrue(daemon.isRunning());
    }

    @Test
    public void daemonCanBeRestarted() {
        // By default, if requested, the daemon should start by default.
        Assert.assertTrue(daemon.isRunning());
        daemon.reset();
        Assert.assertTrue(!daemon.isRunning());
        daemon.start();
        Assert.assertTrue(daemon.isRunning());
    }

    @Test
    public void daemonProperlyUpdates() throws Exception {
        service.registry().register(new SampleHealthyIndicator());
        daemon.start();
        // First Update.
        assertHealthServiceStatus(Status.UP);
        // We register an indicator that will return a failing result, so the next health check should contain
        // a DOWN status.
        service.registry().register(new SampleFailingIndicator());
        assertHealthServiceStatus(Status.DOWN);
    }

    private void assertHealthServiceStatus(Status expected) {
        try {
            AssertExtensions.assertEventuallyEquals(expected,
                    () -> daemon.getLatestHealth().getStatus(),
                    (daemon.getInterval() + 1) * 1000);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
