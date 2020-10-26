/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.RandomStringUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import mesosphere.marathon.client.MarathonException;

import static org.junit.Assert.assertEquals;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class DynamicRestApiTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final Client client;
    private WebTarget webTarget;
    private String restServerURI;
    private String resourceURl;

    public DynamicRestApiTest() {

        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);

        if (Utils.AUTH_ENABLED) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(Utils.PRAVEGA_PROPERTIES.get("pravega.client.auth.username"),
                    Utils.PRAVEGA_PROPERTIES.get("pravega.client.auth.password"));
            clientConfig.register(feature);
        }

        client = ClientBuilder.newClient(clientConfig);
    }

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws InterruptedException If interrupted
     * @throws MarathonException    when error in setup
     * @throws URISyntaxException   If URI is invalid
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Test
    public void listScopes() {
        Service controllerService = Utils.createPravegaControllerService(null);
        List<URI> controllerURIs = controllerService.getServiceDetails();
        URI controllerGRPCUri = controllerURIs.get(0);
        URI controllerRESTUri = controllerURIs.get(1);
        Invocation.Builder builder;
        @Cleanup
        Response response = null;
        String protocol = Utils.TLS_AND_AUTH_ENABLED ? "https://" : "http://";
        restServerURI = protocol + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);

        // Validate the liveliness of the server through a 'ping' request.
        resourceURl = new StringBuilder(restServerURI).append("/ping").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals(String.format("Received unexpected status code: %s in response to 'ping' request.", response.getStatus()),
                OK.getStatusCode(),
                response.getStatus());

        final String scope1 = RandomStringUtils.randomAlphanumeric(10);
        final String stream1 = RandomStringUtils.randomAlphanumeric(10);

        Client client = ClientBuilder.newClient();
        String responseAsString = null;

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerGRPCUri)
                .credentials(new DefaultCredentials(Utils.PRAVEGA_PROPERTIES.get("pravega.client.auth.username"),
                        Utils.PRAVEGA_PROPERTIES.get("pravega.client.auth.username")))
                .build();
        // Create a scope.
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);
        boolean isScopeCreated = streamManager.createScope(scope1);
        assertTrue("Failed to create scope", isScopeCreated);
        // Create a stream.
        boolean isStreamCreated = streamManager.createStream(scope1, stream1, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        assertTrue("Failed to create stream", isStreamCreated);

        // Validate that the scope is returned from the request.
        webTarget = client.target(restServerURI).path("v1").path("scopes");
        builder = webTarget.request();
        response = builder.get();
        assertEquals("Get scopes failed.", OK.getStatusCode(), response.getStatus());
        responseAsString = response.readEntity(String.class);
        assertTrue(responseAsString.contains(String.format("\"scopeName\":\"%s\"", scope1)));

        // Validate that the stream is returned from the request.
        webTarget = client.target(restServerURI).path("v1").path("scopes").path(scope1).path("streams");
        builder = webTarget.request();
        response = builder.get();
        assertEquals("Get streams failed.", OK.getStatusCode(), response.getStatus());
        responseAsString = response.readEntity(String.class);
        assertTrue(responseAsString.contains(String.format("\"streamName\":\"%s\"", stream1)));
    }
}