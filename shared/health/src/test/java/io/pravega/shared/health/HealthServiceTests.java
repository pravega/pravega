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

import io.pravega.shared.health.impl.HealthConfigImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;

@Slf4j
public class HealthServiceTests {

    @Rule
    public final Timeout timeout = new Timeout(600, TimeUnit.SECONDS);

    HealthConfig config = HealthConfigImpl.builder().empty();

    HealthService service;

    public void start() throws IOException {
        HealthProvider.initialize(config);
        service = HealthProvider.getHealthService();
    }

    public void stop() {
        HealthProvider.getHealthService().clear();
        Assert.assertEquals("The HealthService should not maintain any references to HealthContributors.",
                1,
                HealthProvider.getHealthService().components().size());
        Assert.assertEquals("The ContributorRegistry should not maintain any references to HealthContributors",
                1,
                HealthProvider.getHealthService().registry().contributors().size());
    }

    @Before
    @SneakyThrows
    public void before() {
        start();
    }

    @After
    public void after() {
        stop();
    }

    @Test
    public void lifecycle() throws IOException {
        // Perform a start-up and shutdown sequence (implicit start() with @Before).
        stop();
        // Verify that it is repeatable.
        start();
        stop();
    }

    @Test
    public void health() throws IOException {

        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Health health = service.endpoint().health(indicator.getName());
        Assert.assertTrue("Status of the default/root component is expected to be 'UP'", health.getStatus() == Status.UP);

        health = service.endpoint().health(true);
        Assert.assertEquals("There should be exactly one child (SimpleIndicator)", health.getChildren().size(), 1);
    }

    @Test
    public void components() {
       // SampleHealthyIndicator indicator = new SampleHealthyIndicator();
       // service.registry().register(indicator);

       // Collection<HealthContributor> components = service.components();
       // // Only the 'ROOT' HealthComponent should be registered.
       // Assert.assertEquals("No non-root components have been defined.", service.components().size(), 1);
       // // Assert that it is indeed the 'ROOT'
       // Assert.assertEquals("Expected the name of the returned component to match the ROOT's given name.",
       //         ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME,
       //         components.stream().findFirst());
    }

    @Test
    public void details() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Health health = service.endpoint().health(true);
        Assert.assertTrue("There should be at least one child (SimpleIndicator)", health.getChildren().size() >= 1);

        Health sample = health.getChildren().stream().findFirst().get();
        Assert.assertTrue("There should be one details entry provided by the SimpleIndicator.",
                sample.getDetails().size() == 1);
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                sample.getDetails().stream().findFirst().get().getKey(),
                SampleHealthyIndicator.DETAILS_KEY);
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                sample.getDetails().stream().findFirst().get().getValue(),
                SampleHealthyIndicator.DETAILS_VAL);
    }

    @Test
    public void liveness() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean alive = service.endpoint().liveness();
        Assert.assertEquals("The SampleIndicator should produce an 'alive' result.", true, alive);
    }

    @Test
    public void readiness() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean ready = service.endpoint().readiness();
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);

    }

}
