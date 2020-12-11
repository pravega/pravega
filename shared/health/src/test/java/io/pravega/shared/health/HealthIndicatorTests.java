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

import org.junit.Assert;
import org.junit.Test;
import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.DynamicHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.BodylessIndicator;
import io.pravega.shared.health.TestHealthIndicators.ThrowingIndicator;

public class HealthIndicatorTests {

    /**
     * Tests that by default no details will be provided.
     */
    @Test
    public void testHealthWithoutDetails() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        Health health = indicator.getHealthSnapshot();
        Assert.assertEquals("A HealthIndicator should not have any child contributors.", indicator.contributors().size(), 0);
        Assert.assertEquals("Should exactly one detail entry.", 0, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    /**
     * Test a {@link Health} check, but request its details.
     */
    @Test
    public void testHealthWithDetails() {
        // Details implicitly added during class construction.
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        Health health = indicator.getHealthSnapshot(true);
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    /**
     * Tests that details which are created after construction are correctly returned.
     */
    @Test
    public void testHealthDynamicDetails() {
        DynamicHealthyIndicator indicator = new DynamicHealthyIndicator();
        Health health = indicator.getHealthSnapshot(true);
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
        Assert.assertEquals("Detail with key 'DETAILS_KEY' should have an updated value.",
                DynamicHealthyIndicator.DETAILS_VAL,
                health.getDetails().get(SampleHealthyIndicator.DETAILS_KEY));
    }

    /**
     * Tests that a {@link HealthIndicator} which does not implement {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)}
     * will return a default {@link Health} result.
     */
    @Test
    public void testEmptyDoHealthCheckBody() {
        BodylessIndicator indicator = new BodylessIndicator();
        Health health = indicator.getHealthSnapshot();
        Assert.assertEquals("HealthIndicator should have an 'UNKNOWN' Status.", Status.UNKNOWN, health.getStatus());
    }

    /**
     * If the {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)} throws an error, test that it is caught and
     * a {@link Health} result that reflects an 'unhealthy' state is returned.
     */
    @Test
    public void testIndicatorThrows() {
        ThrowingIndicator indicator = new ThrowingIndicator();
        Health health = indicator.getHealthSnapshot();
        Assert.assertEquals("HealthIndicator should have a 'TERMINATED' Status.", Status.DOWN, health.getStatus());
        Assert.assertTrue("HealthIndicator should be not be marked ready OR alive.", !health.isAlive() && !health.isReady());
    }

}
