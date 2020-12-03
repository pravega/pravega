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

import io.pravega.test.common.AssertExtensions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;

@Slf4j
// Hosts much of the same tests that would go into a HealthEndpointTests class, making HealthEndpointTests redundant.
public class HealthServiceTests {

    @Rule
    public final Timeout timeout = new Timeout(600, TimeUnit.SECONDS);

    HealthService service;

    HealthServiceFactory factory;

    public void start() throws IOException {
        factory = new HealthServiceFactory();
        service = factory.createHealthService(true);
    }

    public void stop() {
        service.clear();
        factory.close();
        Assert.assertEquals("The HealthService should not maintain any references to HealthContributors.",
                1,
                service.components().size());
        Assert.assertEquals("The ContributorRegistry should not maintain any references to HealthContributors",
                1,
                service.registry().contributors().size());
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
    public void testLifecycle() throws IOException {
        // Perform a start-up and shutdown sequence (implicit start() with @Before).
        stop();
        // Verify that it is repeatable.
        start();
        stop();
    }

    @Test
    public void testHealth() throws IOException {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);
        try {
            Health health = service.endpoint().health(indicator.getName());
            Assert.assertTrue("Status of the default/root component is expected to be 'UP'", health.getStatus() == Status.UP);

            health = service.endpoint().health(true);
            Assert.assertEquals("There should be exactly one child (SimpleIndicator)", health.getChildren().size(), 1);
        } catch (ContributorNotFoundException e) {
            Assert.fail("HealthContributor not found.");
        }
    }

    @Test
    public void testHealthInvalidName() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);
        AssertExtensions.assertThrows("Health endpoint should throw upon non-existent contributor.",
                () -> service.endpoint().health("unknown-indicator-name"),
                ex -> ex instanceof ContributorNotFoundException);
    }

    // Components are strictly HealthComponent objects, where as 'Dependencies' are *any* subclass of HealthContributor.
    @Test
    public void testComponents() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Collection<String> components = service.components();
        // Only the 'ROOT' HealthComponent should be registered.
        Assert.assertEquals("No non-root components have been defined.", service.components().size(), 1);
        // Assert that it is indeed the 'ROOT'
        Assert.assertEquals("Expected the name of the returned component to match the ROOT's given name.",
                ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME,
                components.stream().findFirst().get());
    }

    @Test
    public void testDependencies() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        // Test 'service level' endpoint.
        List<String> dependencies = service.endpoint().dependencies();
        Assert.assertEquals("There should be exactly one dependency.", 1, dependencies.size());
        Assert.assertEquals("That dependency should be named 'sample-healthy-indicator'.", indicator.getName(), dependencies.stream().findFirst().get());
        // Test Contributor level endpoint.
        dependencies = service.endpoint().dependencies(indicator.getName());
        Assert.assertEquals("The HealthIndicator should not have any dependencies.", 0, dependencies.size());
    }

    @Test
    public void testDetailsEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Health health = service.endpoint().health(true);
        Assert.assertTrue("There should be at least one child (SimpleIndicator)", health.getChildren().size() >= 1);

        // Tests that the Details object(s) are reachable via the 'service level' endpoint.
        Health sample = health.getChildren().stream().findFirst().get();
        Assert.assertEquals("There should be one details entry provided by the SimpleIndicator.", 1, sample.getDetails().size());
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                SampleHealthyIndicator.DETAILS_KEY,
                sample.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                SampleHealthyIndicator.DETAILS_VAL,
                sample.getDetails().entrySet().stream().findFirst().get().getValue());

        // Directly query the HealthContributor endpoint.
        health = service.endpoint().health(indicator.getName(), true);
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                SampleHealthyIndicator.DETAILS_KEY,
                health.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                SampleHealthyIndicator.DETAILS_VAL,
                health.getDetails().get(SampleHealthyIndicator.DETAILS_KEY));
    }

    @Test
    public void testStatusEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Status status = service.endpoint().status();
        // Test the 'service level' endpoint.
        Assert.assertEquals("Status should be UP.", Status.UP, status);
        // Test the Contributor level endpoint.
        status = service.endpoint().status(indicator.getName());
        Assert.assertEquals("Status should be UP.", Status.UP, status);
    }

    @Test
    public void testLivenessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean alive = service.endpoint().liveness();
        Assert.assertEquals("The HealthService should produce an 'alive' result.", true, alive);
        alive = service.endpoint().liveness(indicator.getName());
        Assert.assertEquals("The HealthIndicator should produce an 'alive' result.", true, alive);
    }

    @Test
    public void testReadinessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean ready = service.endpoint().readiness();
        Assert.assertEquals("The HealthService should produce a 'ready' result.", true, ready);
        ready = service.endpoint().readiness(indicator.getName());
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);
    }
}
