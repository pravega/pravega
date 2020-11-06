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

import io.pravega.shared.health.impl.HealthServiceImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.junit.rules.Timeout;

@Slf4j
public class HealthServiceTests {

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    private final Client client;
    private WebTarget target;

    public HealthServiceTests() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    public void start() throws IOException {
        // Service should not start unless explicitly started.
        HealthService service = HealthServiceImpl.INSTANCE;
        Assert.assertFalse(service.running());
        // Verify service is now running.
        service.start();
        Assert.assertTrue(service.running());
    }

    public void stop() {
        HealthService service = HealthServiceImpl.INSTANCE;
        // Verify that service will shutdown as expected.
        service.stop();
        Assert.assertFalse(service.running());
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
        // Perform a start-up and shutdown sequence.
        start();
        stop();
        // Verify that it is repeatable.
        start();
        stop();
    }

    @Test
    public void ping() {
        Invocation.Builder builder;
        Response response;

        HealthService service = HealthServiceImpl.INSTANCE;
        Assert.assertTrue(service.running());

        Request request = new Request("ping");
        response = request.get();

        Assert.assertEquals(String.format("[Ping] %s responded with unexpected code.", request.getUrl()),
                Response.Status.OK.getStatusCode(),
                response.getStatus());
        Assert.assertEquals(String.format("[Ping] %s responded with unexpected plain-text.", request.getUrl()),
                HealthEndpoint.PING_RESPONSE,
                response.readEntity(String.class));
    }

    @Test
    public void health() throws IOException {
        HealthService service = HealthServiceImpl.INSTANCE;
        Assert.assertTrue(service.running());

        SampleIndicator indicator = new SampleIndicator();
        service.register(indicator);

        Request request = new Request("health");
        Response response = request.get();
        Health health = response.readEntity(Health.class);
        Assert.assertTrue("Status of the ROOT component is expected to be 'UP'", health.getStatus() == Status.UP);

        request = new Request("health?details=true");
        response = request.get();
        health = response.readEntity(Health.class);
        Assert.assertTrue("There should be at least one child (SimpleIndicator)", health.getChildren().size() >= 1);

        //for (;;) { }
    }

    @Test
    public void components() {

    }

    @Test
    public void details() {

    }

    @Test
    public void liveness() {

    }

    @Test
    public void readiness() {

    }

    private class SampleIndicator extends HealthIndicator {
        public SampleIndicator() {
            super("sample-indicator");
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            builder.status(Status.UP);
        }
    }

    private class Request {

        String url;

        Request(String path) {
            String uri = HealthServiceImpl.INSTANCE.getUri().toString();
            url = new StringBuilder(uri).append(path).toString();
        }

        Response get() {
            log.info("HealthService request sent to: {}", url);
            target = client.target(url);
            Invocation.Builder builder = target.request();
            return builder.get();
        }

        String getUrl() {
            return url;
        }

    }
}
