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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;

/**
 * The {@link HealthServiceTests} encapsulates much of the same processes that {@link HealthEndpoint} performs, so
 * an explicit test class for the former is skipped.
 */
@Slf4j
public class HealthServiceTests {

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    HealthService service;

    HealthServiceFactory factory;

    private void start() {
        factory = new HealthServiceFactory();
        service = factory.createHealthService(true);
    }

    private void stop() {
        service.close();
        factory.close();
        // Except its root.
        Assert.assertEquals("The HealthService should not maintain any references to HealthContributors.",
                1,
                service.components().size());
        // Except its root.
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

    /**
     * Test that the {@link HealthService} is able to be stopped and restarted.
     */
    @Test
    public void testLifecycle() {
        // Perform a start-up and shutdown sequence (implicit start() with @Before).
        stop();
        // Verify that it is repeatable.
        start();
        stop();
    }

    /**
     * Tests that the {@link HealthService} is able to provide {@link Health} results for both a specific {@link HealthIndicator}
     * and the service itself ({@link ContributorRegistry#getRootContributor()}.
     */
    @Test
    public void testHealth() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Health health = service.endpoint().getHealth(indicator.getName());
        Assert.assertTrue("Status of the default/root component is expected to be 'UP'", health.getStatus() == Status.UP);

        health = service.endpoint().getHealth(true);
        Assert.assertEquals("There should be exactly one child (SimpleIndicator)", health.getChildren().size(), 1);
    }

    /**
     * Tests that an appropriate exception is thrown when an invalid {@link HealthContributor} id/name is provided.
     */
    @Test
    public void testHealthInvalidName() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);
        AssertExtensions.assertThrows("Health endpoint should throw upon non-existent contributor.",
                () -> service.endpoint().getHealth("unknown-indicator-name"),
                ex -> ex instanceof ContributorNotFoundException);
    }


    /**
     * Tests that the {@link HealthService} is properly able to distinguish between those {@link HealthContributor}
     * which are {@link io.pravega.shared.health.impl.HealthComponent} and those that are {@link HealthIndicator}.
     *
     */
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

    /**
     * Tests that all the {@link HealthContributor} objects that are used to arrive at a {@link Health} decision for another
     * {@link HealthContributor} are provided.
     *
     * Note: A 'dependency' is any sub-class of {@link HealthContributor}.
     */
    @Test
    public void testDependencies() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        // Test 'service level' endpoint.
        List<String> dependencies = service.endpoint().getDependencies();
        Assert.assertEquals("There should be exactly one dependency.", 1, dependencies.size());
        Assert.assertEquals("That dependency should be named 'sample-healthy-indicator'.", indicator.getName(), dependencies.stream().findFirst().get());
        // Test Contributor level endpoint.
        dependencies = service.endpoint().getDependencies(indicator.getName());
        Assert.assertEquals("The HealthIndicator should not have any dependencies.", 0, dependencies.size());
    }

    /**
     * Tests that when specified a {@link Health} result will return any details information belonging to that
     * {@link HealthContributor} or it's dependencies.
     *
     * A {@link io.pravega.shared.health.impl.HealthComponent} does not directly supply any details information,
     * but any dependency (that is of type {@link HealthIndicator}) that it has should supply them.
     */
    @Test
    public void testDetailsEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Health health = service.endpoint().getHealth(true);
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
        health = service.endpoint().getHealth(indicator.getName(), true);
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                SampleHealthyIndicator.DETAILS_KEY,
                health.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                SampleHealthyIndicator.DETAILS_VAL,
                health.getDetails().get(SampleHealthyIndicator.DETAILS_KEY));
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its {@link Status}
     * information.
     */
    @Test
    public void testStatusEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        Status status = service.endpoint().getStatus();
        // Test the 'service level' endpoint.
        Assert.assertEquals("Status should be UP.", Status.UP, status);
        // Test the Contributor level endpoint.
        status = service.endpoint().getStatus(indicator.getName());
        Assert.assertEquals("Status should be UP.", Status.UP, status);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its liveness information.
     */
    @Test
    public void testLivenessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean alive = service.endpoint().isAlive();
        Assert.assertEquals("The HealthService should produce an 'alive' result.", true, alive);
        alive = service.endpoint().isAlive(indicator.getName());
        Assert.assertEquals("The HealthIndicator should produce an 'alive' result.", true, alive);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its readiness information.
     */
    @Test
    public void testReadinessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.registry().register(indicator);

        boolean ready = service.endpoint().isReady();
        Assert.assertEquals("The HealthService should produce a 'ready' result.", true, ready);
        ready = service.endpoint().isReady(indicator.getName());
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);
    }
}
