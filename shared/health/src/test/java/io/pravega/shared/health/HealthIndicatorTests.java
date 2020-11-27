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

import org.junit.Assert;
import org.junit.Test;
import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.DynamicHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.BodylessIndicator;
import io.pravega.shared.health.TestHealthIndicators.ThrowingIndicator;

public class HealthIndicatorTests {

    // A HealthIndicator should be have any contributors (dependencies).
    @Test
    public void testHealthWithoutDetails() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        Health health = indicator.health(false);
        Assert.assertEquals("A HealthIndicator should not have any child contributors.", indicator.contributors().size(), 0);
        Assert.assertEquals("Should exactly one detail entry.", 0, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    // Provide details information in constructor.
    @Test
    public void testHealthWithDetails() {
        // Details implicitly added during class construction.
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        Health health = indicator.health(true);
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    @Test
    // Set details during the `doHealthCheck`.
    public void testHealthDynamicDetails() {
        DynamicHealthyIndicator indicator = new DynamicHealthyIndicator();
        Health health = indicator.health(true);
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
        Assert.assertEquals("Detail with key 'DETAILS_KEY' should have an updated value.",
                DynamicHealthyIndicator.DETAILS_VAL,
                health.getDetails().get(SampleHealthyIndicator.DETAILS_KEY));
    }

    @Test
    public void testEmptyDoHealthCheckBody() {
        BodylessIndicator indicator = new BodylessIndicator();
        Health health = indicator.health();
        Assert.assertEquals("HealthIndicator should have an 'UNKNOWN' Status.", Status.UNKNOWN, health.getStatus());
    }

    @Test
    public void testIndicatorThrows() {
        ThrowingIndicator indicator = new ThrowingIndicator();
        Health health = indicator.health();
        Assert.assertEquals("HealthIndicator should have a 'DOWN' Status.", Status.DOWN, health.getStatus());
        Assert.assertTrue("HealthIndicator should be not be marked ready OR alive.", !health.isAlive() && !health.isReady());
    }

}
