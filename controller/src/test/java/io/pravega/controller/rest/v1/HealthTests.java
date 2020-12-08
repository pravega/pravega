/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rest.RESTServer;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.HealthDependencies;
import io.pravega.controller.server.rest.generated.model.HealthDetails;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.HealthIndicator;
import io.pravega.shared.health.HealthService;

import io.pravega.shared.health.HealthServiceFactory;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.HealthConfigImpl;
import io.pravega.shared.health.impl.StatusAggregatorImpl;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

/**
 * The {@link HealthTests} can be divided into two sets: those that test the implicit component (root) that is automatically
 * created alongside the {@link io.pravega.shared.health.ContributorRegistry} and those that test specific (non-root) HealthComponents.
 *
 * Both implicit and explicit requests have the same code path(s) so we can expect results to be similar between each
 * class of tests.
 */
@Slf4j
public class HealthTests {

    private static final String HOST = "localhost";

    private static final String IMPLICIT_INDICATOR = "implicit-health-child";

    private static final String IMPLICIT_COMPONENT = "health-component";

    //Ensure each test completes within 30 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(10000, TimeUnit.SECONDS);

    private  HealthServiceFactory healthServiceFactory;

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private Client client;
    private ConnectionFactory connectionFactory;
    private HealthService service;

    @Before
    public void setup() throws Exception {
        ControllerService mockControllerService = mock(ControllerService.class);
        serverConfig = getServerConfig();
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        healthServiceFactory = new HealthServiceFactory(getHealthConfig());
        service = healthServiceFactory.createHealthService(true);
        restServer = new RESTServer(null, mockControllerService, null, serverConfig,
                connectionFactory, service);
        restServer.startAsync();
        restServer.awaitRunning();
        client = createJerseyClient();
        // Must provide a HealthIndicator to the HealthComponent(s) defined by the HealthConfig.
        service.registry().register(new StaticHealthyIndicator(IMPLICIT_INDICATOR), IMPLICIT_COMPONENT);
    }

    @After
    public void tearDown() {
        service.clear();
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
        connectionFactory.close();
        healthServiceFactory.close();
        // The ROOT component + the component supplied by our HealthConfig (getHealthConfig).
        Assert.assertEquals(2, service.registry().contributors().size());
        Assert.assertEquals(2, service.registry().components().size());
        // Assert that the component does not contain children.
        Assert.assertEquals("The HealthService should report only one component.",
                1,
                service.registry().getRootContributor().contributors().size());
    }

    protected Client createJerseyClient() throws Exception {
        return ClientBuilder.newClient();
    }

    RESTServerConfig getServerConfig() throws Exception {
        return RESTServerConfigImpl.builder().host(HOST).port(TestUtils.getAvailableListenPort())
                .build();
    }

    HealthConfig getHealthConfig() throws Exception {
        return HealthConfigImpl.builder()
                .define("health-component", StatusAggregatorImpl.DEFAULT)
                .build();
    }

    String getURLScheme() {
        return "http";
    }

    protected URI getURI(String path) {
        return UriBuilder.fromPath(path)
                .scheme(getURLScheme())
                .host(HOST)
                .port(serverConfig.getPort())
                .build();
    }

    @Test
    public void testHealthNoContributors() {
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", IMPLICIT_COMPONENT)))
                .scheme(getURLScheme()).build();
        // Remove the only contributor of 'IMPLICIT_COMPONENT'.
        service.registry().unregister(IMPLICIT_INDICATOR);

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        response.close();

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("Health Status should be 'UNKNOWN'", HealthStatus.UNKNOWN, healthResult.getStatus());
    }

    @Test
    public void testHealth()  {
        // Register the HealthIndicator.
        service.registry().register(new StaticHealthyIndicator());

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);

        log.info("{}", healthResult);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("HealthService should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
        Assert.assertTrue("HealthService should not provide details.", firstChild(healthResult).getDetails().isEmpty());

        // Test *with* details.
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
                .scheme(getURLScheme()).queryParam("details", true).build();
        response = client.target(streamResourceURI).request().buildGet().invoke();
        healthResult = response.readEntity(HealthResult.class);
        boolean found = false;
        for (HealthResult child : healthResult.getChildren()) {
            found |= !child.getDetails().isEmpty();
        }
        Assert.assertTrue("HealthService should provide details.", found);
    }

    @Test
    public void testContributorHealth() {
        // Register the HealthIndicator.
        StaticHealthyIndicator indicator = new StaticHealthyIndicator();
        service.registry().register(indicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", indicator.getName())))
                .scheme(getURLScheme()).build();
        log.info("{}", streamResourceURI);
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("The HealthIndicator should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
        Assert.assertTrue("The HealthIndicator should not provide details.", healthResult.getDetails().isEmpty());
    }

    @Test
    // Be mindful that the results will be dependent on the StatusAggregatorRule used.
    public void testStatus()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.UP);

        // Adding an unhealthy indicator should change the Status.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.registry().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);

        // Make sure that even though we have a majority of healthy reports, we still are considered failing.
        service.registry().register(new StaticHealthyIndicator("sample-healthy-indicator-two"));
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);
    }

    @Test
    public void testContributorStatus() {
        DynamicIndicator dynamicIndicator = new DynamicIndicator(new ArrayList<>(Arrays.asList(
                builder -> builder.status(Status.UP),
                builder -> builder.status(Status.DOWN),
                builder -> builder.status(Status.UNKNOWN)
        )));
        service.registry().register(dynamicIndicator);
        // Target a specific indicator via a PathParam.
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/status/%s", dynamicIndicator.getName())))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.UP);
        // Should now return Status.DOWN.
        assertStatus(streamResourceURI, HealthStatus.DOWN);
        // No body provided for the third health check call, so it should be in an 'UNKNOWN' Status.
        assertStatus(streamResourceURI, HealthStatus.UNKNOWN);
    }

    // Service Readiness is in essence a proxy for the Status as a HealthComponent does not provide its own
    // `doHealthCheck` logic.
    @Test
    public void testReadiness()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy indicator should change the readiness.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.registry().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testContributorReadiness() {
        DynamicIndicator dynamicIndicator = new DynamicIndicator(new ArrayList<>(Arrays.asList(
                builder -> {
                    builder.status(Status.UP);
                    builder.ready(true);
                },
                builder -> {
                    builder.status(Status.UP);
                    builder.ready(false);
                },
                // Set the Contributor into a logically invalid state ('DOWN') when ready.
                builder -> {
                    builder.status(Status.DOWN);
                    builder.ready(true);
                }
        )));
        service.registry().register(dynamicIndicator);
        // Target a specific indicator via a PathParam.
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/readiness/%s", dynamicIndicator.getName())))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);
        // Should now return Status.DOWN.
        assertAliveOrReady(streamResourceURI, false);

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testLiveness()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy indicator should change the readiness.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.registry().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testContributorLiveness() {
        DynamicIndicator dynamicIndicator = new DynamicIndicator(new ArrayList<>(Arrays.asList(
                builder -> {
                    builder.status(Status.UP);
                    builder.alive(true);
                },
                // Set the Contributor into a logically invalid state ('DOWN') when ready.
                builder -> {
                    builder.status(Status.DOWN);
                    builder.alive(true);
                }
        )));
        service.registry().register(dynamicIndicator);
        // Target a specific indicator via a PathParam.
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/liveness/%s", dynamicIndicator.getName())))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testDependencies()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);
        HealthDependencies  expected = new HealthDependencies();
        expected.add(IMPLICIT_COMPONENT);
        expected.add(healthyIndicator.getName());
        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/components"))
                .scheme(getURLScheme()).build();
        assertDependencies(streamResourceURI, expected);
        // Add another HealthIndicator.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.registry().register(failingIndicator);
        expected.add(failingIndicator.getName());
        assertDependencies(streamResourceURI, expected);
    }

    @Test
    public void testContributorDependencies() {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/components/%s", healthyIndicator.getName())))
                .scheme(getURLScheme()).build();
        // HealthIndicators should not be able to accept other HealthIndicators.
        assertDependencies(streamResourceURI, new HealthDependencies());
    }

    @Test
    public void testDetails()  {
        // Register the HealthIndicator.
        service.registry().register(new StaticHealthyIndicator());
        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/details"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthDetails details = response.readEntity(HealthDetails.class);
        Assert.assertTrue("HealthService does not provide details itself.", details.isEmpty());
    }

    @Test
    public void testContributorDetails() {
        // Register the HealthIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.registry().register(healthyIndicator);
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/details/%s", healthyIndicator.getName())))
                .scheme(getURLScheme()).build();
        HealthDetails expected = new HealthDetails();
        expected.put(StaticHealthyIndicator.DETAILS_KEY, StaticHealthyIndicator.DETAILS_VAL);
        assertDetails(streamResourceURI, expected);
    }

    @Test
    public void testContributorNotExists() {
        String unknown = "unknown-indicator";
        // Register the HealthIndicator.
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", unknown)))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        Assert.assertEquals(404, response.getStatus());
    }

    private void assertDetails(URI uri, HealthDetails expected) {
        Response response = client.target(uri).request().buildGet().invoke();
        HealthDetails details = response.readEntity(HealthDetails.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("Size of HealthDetails result should match the expected.", expected.size(), details.size());
        details.forEach((key, val) -> {
            if (!expected.get(key).equals(val)) {
                Assert.assertEquals("Unexpected difference in Detail entry.", expected.get(key), val);
            }
        });
        response.close();
    }

    private void assertStatus(URI uri, HealthStatus expected) {
        Response response = client.target(uri).request().buildGet().invoke();
        HealthStatus status = response.readEntity(HealthStatus.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(String.format("The StaticHealthyIndicator should be in a '%s' Status.", expected.toString()),
                expected,
                status);
        response.close();
    }

    private void assertAliveOrReady(URI uri, boolean expected) {
        Response response = client.target(uri).request().buildGet().invoke();
        boolean ready = response.readEntity(Boolean.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(String.format("The StaticHealthyIndicator should be %s.", expected ? "alive/ready" : "not alive/ready"),
                expected,
                ready);
        response.close();
    }

    private void assertDependencies(URI uri, HealthDependencies expected) {
        Response response = client.target(uri).request().buildGet().invoke();
        HealthDependencies dependencies = response.readEntity(HealthDependencies.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("'expected' and 'actual' differ in sizes.", expected.size(), dependencies.size());
        // Normalize arrays for comparison.
        Collections.sort(expected);
        Collections.sort(dependencies);
        Assert.assertArrayEquals(expected.toArray(), dependencies.toArray());
        response.close();
    }


    private HealthResult firstChild(HealthResult result) {
        if (result.getChildren().isEmpty()) {
            throw new RuntimeException("HealthResult was expected to contain children.");
        }
        return result.getChildren().stream().findFirst().get();
    }

    private static class StaticHealthyIndicator extends HealthIndicator {
        public static final String DETAILS_KEY = "static-indicator-details-key";

        public static final String DETAILS_VAL = "static-indicator-details-value";


        public StaticHealthyIndicator() {
            super("static-healthy-indicator");
        }

        public StaticHealthyIndicator(String name) {
            super(name);
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setDetail(DETAILS_KEY, () -> DETAILS_VAL);
            builder.status(Status.UP);
            builder.alive(true);
        }
    }

    // To avoid relying on potentially complex logic of some service, supply a list of actions that helps us
    // model changing health state of the indicator.
    private static class DynamicIndicator extends StaticHealthyIndicator {
        private int id = 0;
        private List<Consumer<Health.HealthBuilder>> actions = new ArrayList<>();

        DynamicIndicator(List<Consumer<Health.HealthBuilder>> actions) {
            super("dynamic-health-indicator");
            this.actions = actions;
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            actions.get(id++).accept(builder);
        }
    }

    private static class StaticFailingIndicator extends HealthIndicator {
        public StaticFailingIndicator() {
            super("static-failing-indicator");
        }

        public StaticFailingIndicator(String name) {
            super(name);
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            builder.status(Status.DOWN);
            builder.alive(false);
        }
    }
}
