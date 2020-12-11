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

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class HealthComponentTests {

    private ContributorRegistry registry;

    @Before
    public void before() {
        registry = new ContributorRegistryImpl();
    }

    @After
    public void after() {
        registry.clear();
    }

    /**
     * Tests health checking logic when *only* {@link HealthComponent} objects have been defined, with no {@link io.pravega.shared.health.HealthIndicator}
     * objects to supply non-default {@link Health} results.
     */
    @Test
    public void testOnlyComponents() {
        // Health after initialization.
        Health health = registry.getRootContributor().getHealthSnapshot();
        Assert.assertEquals("A HealthComponent with no dependencies should provide an 'UNKNOWN' Status.",
                Status.UNKNOWN,
                health.getStatus());
        // Register a child component.
        HealthComponent component = new HealthComponent("child", StatusAggregatorImpl.DEFAULT, registry);
        registry.register(component);
        // The HealthComponent itself should be in a 'UNKNOWN' state.
        health = component.getHealthSnapshot();
        Assert.assertEquals("A HealthComponent with no HealthIndicators should provide an 'UNKNOWN' Status.",
                Status.UNKNOWN,
                health.getStatus());
        // Now that we are querying at the root/service level, it has dependencies which don't return 'UP' indicating
        // things are not healthy are the service level.
        health = registry.getRootContributor().getHealthSnapshot();
        Assert.assertEquals("A HealthComponent with UNKNOWN dependencies should provide a 'DOWN' Status.",
                Status.DOWN,
                health.getStatus());
    }

    /**
     * Tests health checking functionality with a {@link io.pravega.shared.health.HealthIndicator}.
     */
    @Test
    public void testWithIndicator() {
        // Define HealthService layout.
        HealthComponent parent = new HealthComponent("parent", StatusAggregatorImpl.DEFAULT, registry);
        HealthComponent child = new HealthComponent("child", StatusAggregatorImpl.DEFAULT, registry);
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();

        // Should fail registration.
        HealthContributor result = registry.register(child, parent);
        Assert.assertEquals("Returned HealthContributor should be 'null'.", null, result);
        // Now register in proper order.
        registry.register(parent);
        registry.register(child, parent);
        // Check Health *before* any HealthIndicators are added.
        Health health = child.getHealthSnapshot();
        Assert.assertEquals("HealthComponent should return an 'UNKNOWN' Status.", Status.UNKNOWN, health.getStatus());
        registry.register(indicator, child);
        // Should now provide a healthy result.
        health = child.getHealthSnapshot(true);
        Assert.assertEquals("The HealthComponent should now be healthy ('UP').", Status.UP, health.getStatus());
        // We asked for details, so check they are also returned. Parent -> Child -> Indicator.
        Assert.assertEquals("HealthIndicator should list its exported details.", true,
                health.getChildren().stream().findFirst().get().getDetails().size() > 0);
        // Now check that they are not exported.
        health = child.getHealthSnapshot(false);
        Assert.assertEquals("HealthIndicator should not list its exported details.", true,
                health.getChildren().isEmpty());
        // Verify that the only (direct) dependency on the 'root' component is 'parent'.
        Collection<HealthContributor> dependencies = registry.dependencies();
        Assert.assertEquals("Dependency size should be 1.", 1, dependencies.size());
        Assert.assertEquals("That dependency should have name 'parent'.", parent.getName(), dependencies.stream().findFirst().get().getName());
    }
}
