/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rest.RESTServer;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthIndicator;
import io.pravega.shared.health.HealthProvider;
import io.pravega.shared.health.HealthService;

import io.pravega.shared.health.Status;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;

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

    //Ensure each test completes within 30 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(100, TimeUnit.SECONDS);

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private Client client;
    private ConnectionFactory connectionFactory;
    private HealthService service;

    @BeforeClass
    public static void before() {
        HealthProvider.initialize();
    }

    @Before
    public void setup() throws Exception {
        ControllerService mockControllerService = mock(ControllerService.class);
        serverConfig = getServerConfig();
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        restServer = new RESTServer(null, mockControllerService, null, serverConfig,
                connectionFactory);
        restServer.startAsync();
        restServer.awaitRunning();
        client = createJerseyClient();
        service = HealthProvider.getHealthService();
    }

    @After
    public void teardown() {
        service.clear();
        // Only the root component should be left.
        Assert.assertEquals(service.registry().contributors().size(), 1);
        Assert.assertEquals(service.registry().components().size(), 1);
        // Assert that the component does not contain children.
        Assert.assertTrue(service.registry().get().get().contributors().isEmpty());
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

        @After
    public void tearDown() {
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
        connectionFactory.close();
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
        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
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
        service.registry().register(new SampleHealthyIndicator());

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
        log.info("{}", healthResult);
        Assert.assertTrue("HealthService should provide details.", !firstChild(healthResult).getDetails().isEmpty());
    }

    @Test
    public void testContributorHealth() {
        // Register the HealthIndicator.
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
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
        SampleHealthyIndicator healthyIndicator = new SampleHealthyIndicator();
        service.registry().register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthStatus healthResult = response.readEntity(HealthStatus.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("The HealthService should maintain an 'UP' Status.", HealthStatus.UP, healthResult);

        // Adding an unhealthy indicator should change the Status.
        SampleFailingIndicator failingIndicator = new SampleFailingIndicator();
        service.registry().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        response = client.target(streamResourceURI).request().buildGet().invoke();
        healthResult = response.readEntity(HealthStatus.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("The HealthService should now report a 'DOWN' Status.", HealthStatus.DOWN, healthResult);

        // Make sure that even though we have a majority of healthy reports, we still are considered failing.
        service.registry().register(new SampleHealthyIndicator("sample-healthy-indicator-two"));
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        response = client.target(streamResourceURI).request().buildGet().invoke();
        healthResult = response.readEntity(HealthStatus.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("The HealthService should still report a 'DOWN' Status.", HealthStatus.DOWN, healthResult);
    }

    @Test
    public void testContributorStatus() {

    }

    @Test
    public void testReadiness()  {

    }

    @Test
    public void testContributorReadiness() {

    }

    @Test
    public void testLiveness()  {

    }

    @Test
    public void testContributorLiveness() {

    }

    @Test
    public void testDependencies()  {

    }

    @Test
    public void testContributorDependencies() {

    }

    @Test
    public void testDetails()  {

    }

    @Test
    public void testContributorDetails() {

    }

    private HealthResult firstChild(HealthResult result) {
        if (result.getChildren().isEmpty()) {
            throw new RuntimeException("HealthResult was expected to contain children.");
        }
        return result.getChildren().stream().findFirst().get();
    }

    private static class SampleHealthyIndicator extends HealthIndicator {
        public static final String DETAILS_KEY = "sample-indicator-details-key";

        public static final String DETAILS_VAL = "sample-indicator-details-value";

        public SampleHealthyIndicator() {
            super("sample-healthy-indicator");
        }

        public SampleHealthyIndicator(String name) {
            super(name);
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setDetail(DETAILS_KEY, () -> DETAILS_VAL);
            builder.status(Status.UP);
            builder.alive(true);
        }
    }


    private static class SampleFailingIndicator extends HealthIndicator {

        public SampleFailingIndicator() {
            super("sample-failing-indicator");
        }

        public SampleFailingIndicator(String name) {
            super(name);
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            builder.status(Status.DOWN);
            builder.alive(false);
        }
    }
}
