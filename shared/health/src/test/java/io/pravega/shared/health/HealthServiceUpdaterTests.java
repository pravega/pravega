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

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.SampleFailingIndicator;

import io.pravega.test.common.AssertExtensions;

import java.time.Duration;

@Slf4j
public class HealthServiceUpdaterTests {

    public static final Duration TIMEOUT = Duration.ofSeconds(10);

    HealthService service;

    HealthServiceUpdater healthServiceUpdater;

    HealthServiceFactory factory;
    @Before
    public void before() {
        factory = new HealthServiceFactory();
        service = factory.createHealthService(true);
        healthServiceUpdater = service.getHealthServiceUpdater();
        healthServiceUpdater.awaitRunning();
    }

    @After
    public void after() {
        factory.close();
        service.close();
        healthServiceUpdater.stopAsync();
        healthServiceUpdater.awaitTerminated();
    }

    @Test
    public void testIsRunningAfterServiceInitialization() {
        Assert.assertTrue(healthServiceUpdater.isRunning());
    }

    @Test
    public void testServiceUpdaterProperlyUpdates() throws Exception {
        service.registry().register(new SampleHealthyIndicator());
        // First Update.
        assertHealthServiceStatus(Status.UP);
        // We register an indicator that will return a failing result, so the next health check should contain a 'DOWN' Status.
        service.registry().register(new SampleFailingIndicator());
        assertHealthServiceStatus(Status.DOWN);
    }

    private void assertHealthServiceStatus(Status expected) {
        try {
            AssertExtensions.assertEventuallyEquals(expected,
                    () -> healthServiceUpdater.getLatestHealth().getStatus(),
                    (healthServiceUpdater.getInterval() + 1) * 1000);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
