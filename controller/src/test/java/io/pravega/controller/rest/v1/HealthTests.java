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
import io.pravega.controller.server.rest.generated.model.Status;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.test.common.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HealthTests {

    private static final String HOST = "localhost";

    //Ensure each test completes within 30 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private Client client;
    private ConnectionFactory connectionFactory;

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
        System.out.println(streamResourceURI);
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);

        assertEquals(200, response.getStatus());
        assertEquals("Health Status should be 'UNKNOWN'", healthResult.getStatus(), Status.UNKNOWN);
    }

    @Test
    public void testHealth()  {
        URI streamResourceURI = UriBuilder.fromUri(getURI("health"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);

        assertEquals(200, response.getStatus());
    }

    @Test
    public void testContributorHealth() {
    }

    @Test
    public void testStatus()  {

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
}
