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
import io.pravega.shared.health.HealthService;

import io.pravega.shared.health.HealthServiceFactory;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

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
        healthServiceFactory = new HealthServiceFactory();
        service = healthServiceFactory.createHealthService();
        restServer = new RESTServer(null, mockControllerService, null, serverConfig,
                connectionFactory,  ClientConfig.builder().build(), service);
        restServer.startAsync();
        restServer.awaitRunning();
        client = createJerseyClient();
        // Must provide a HealthIndicator to the HealthComponent(s) defined by the HealthConfig.
        service.getRoot().register(new StaticHealthyIndicator(IMPLICIT_INDICATOR));
    }

    @After
    public void tearDown() {
        service.close();
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
        connectionFactory.close();
        healthServiceFactory.close();
    }

    protected Client createJerseyClient() throws Exception {
        return ClientBuilder.newClient();
    }

    RESTServerConfig getServerConfig() throws Exception {
        return RESTServerConfigImpl.builder().host(HOST).port(TestUtils.getAvailableListenPort())
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

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        response.close();

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("Health Status should be 'UNKNOWN'", HealthStatus.UNKNOWN, healthResult.getStatus());
    }

    @Test
    public void testHealth()  {
        // Register the HealthIndicator.
        service.getRoot().register(new StaticHealthyIndicator());

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("HealthService should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
        // Details not requested, so children will not be populated.
        Assert.assertTrue("HealthService should not provide children.", healthResult.getChildren().isEmpty());

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
        service.getRoot().register(indicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", indicator.getName())))
                .scheme(getURLScheme()).build();
        log.info("{}", streamResourceURI);
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("The HealthIndicator should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
        Assert.assertTrue("The HealthIndicator should not provide details.", healthResult.getDetails().isEmpty());
    }

    // Be mindful that the results will be dependent on the StatusAggregatorRule used.
    @Test
    public void testStatus()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.getRoot().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.UP);

        // Adding an unhealthy indicator should change the Status.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.getRoot().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);

        // Make sure that even though we have a majority of healthy reports, we still are considered failing.
        service.getRoot().register(new StaticHealthyIndicator("sample-healthy-indicator-two"));
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);
    }

    // Service Readiness is in essence a proxy for the Status as a HealthComponent does not provide its own
    // `doHealthCheck` logic.
    @Test
    public void testReadiness()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.getRoot().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy indicator should change the readiness.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.getRoot().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testLiveness()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.getRoot().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy indicator should change the readiness.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.getRoot().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testDependencies()  {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.getRoot().register(healthyIndicator);
        HealthDependencies  expected = new HealthDependencies();
        expected.add(IMPLICIT_COMPONENT);
        expected.add(healthyIndicator.getName());
        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/components"))
                .scheme(getURLScheme()).build();
        assertDependencies(streamResourceURI, expected);
        // Add another HealthIndicator.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        service.getRoot().register(failingIndicator);
        expected.add(failingIndicator.getName());
        assertDependencies(streamResourceURI, expected);
    }

    @Test
    public void testContributorDependencies() {
        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        service.getRoot().register(healthyIndicator);
        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/components/%s", healthyIndicator.getName())))
                .scheme(getURLScheme()).build();
        // HealthIndicators should not be able to accept other HealthIndicators.
        assertDependencies(streamResourceURI, new HealthDependencies());
    }

    @Test
    public void testDetails()  {
        // Register the HealthIndicator.
        service.getRoot().register(new StaticHealthyIndicator());
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
        service.getRoot().register(healthyIndicator);
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


    private static class StaticHealthyIndicator extends AbstractHealthContributor {
        public static final String DETAILS_KEY = "static-indicator-details-key";

        public static final String DETAILS_VAL = "static-indicator-details-value";


        public StaticHealthyIndicator() {
            super("static-healthy-indicator");
        }

        public StaticHealthyIndicator(String name) {
            super(name);
        }

        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }

    private static class StaticFailingIndicator extends AbstractHealthContributor {

        public static final String DETAILS_KEY = "static-failing-indicator-details-key";

        public static final String DETAILS_VAL = "static-failing-indicator-details-value";

        public StaticFailingIndicator() {
            super("static-failing-indicator");
        }

        public StaticFailingIndicator(String name) {
            super(name);
        }

        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }
}