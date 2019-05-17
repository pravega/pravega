/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.integration.utils.PasswordAuthHandlerInput;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

/**
 * The tests in this class are intended to verify whether Batch Client works with a Pravega cluster
 * that has "Auth" (short for authentication and authorization) enabled.
 *
 * This class inherits the tests of the parent class. Some of the test methods of the parent are reproduced here as
 * handles, to enable running an individual test interactively (for debugging purposes).
 */
@Slf4j
public class BatchClientAuthTest extends BatchClientTest {

    private static final File PASSWORD_AUTHHANDLER_INPUT = createAuthFile();

    @AfterClass
    public static void classTearDown() {
        if (PASSWORD_AUTHHANDLER_INPUT.exists()) {
            PASSWORD_AUTHHANDLER_INPUT.delete();
        }
    }

    @Override
    protected ClientConfig createClientConfig() {
        return ClientConfig.builder()
                    .controllerURI(URI.create(this.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
    }

    @Override
    protected ServiceBuilder createServiceBuilder() {
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1))
                .include(AutoScalerConfig.builder()
                        .with(AutoScalerConfig.CONTROLLER_URI, this.controllerUri())
                        .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                        .with(AutoScalerConfig.AUTH_ENABLED, true));

        return ServiceBuilder.newInMemoryBuilder(configBuilder.build());
    }

    protected ControllerWrapper createControllerWrapper() {
        return new ControllerWrapper(zkTestServer.getConnectString(),
                false, true,
                controllerPort, serviceHost, servicePort, containerCount, -1,
                true, PASSWORD_AUTHHANDLER_INPUT.getPath(), "secret");
    }

    @Test
    public void testAuthWithParamsSpecifiedAsSystemProperties() throws ExecutionException, InterruptedException {

        // Set up client credentials via system properties.
        //
        // Note: setting system properties here is safe for now, even if the tests are run in parallel. Here's why:
        //   - The properties being set here will not override the Credentials specified in ClientConfig object used
        //     in other tests, owing to the resolution order used in ClientConfig (read '>' as overrides):
        //          supplied credentials object > system properties > environment variables
        //     See the respective comments in class ClientConfig.
        //
        //   - There is no other test here that sets a different set of credentials via system properties.
        //
        //  Should either of these assumptions change, this test will need to be revisited to avoid any side effects for
        //  other tests.
        setClientAuthProperties("admin", "1111_aaaa");

        ClientConfig config = ClientConfig.builder()
                .controllerURI(URI.create(this.controllerUri()))
                .build();

        this.listAndReadSegmentsUsingBatchClient("testScope", "testBatchStream", config);

        // Cleanup auth system properties
        unsetClientAuthProperties();
    }

    private static File createAuthFile() {
        PasswordAuthHandlerInput result = new PasswordAuthHandlerInput("BatchClientAuth", ".txt");

        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");
            result.postEntry(PasswordAuthHandlerInput.Entry.of("admin", encryptedPassword, "*,READ_UPDATE;"));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
        return result.getFile();
    }

    private void setClientAuthProperties(String userName, String password) {
        // Prepare the token to be used for basic authentication
        String plainToken = userName + ":" + password;
        String base66EncodedToken = Base64.getEncoder().encodeToString(plainToken.getBytes(StandardCharsets.UTF_8));

        System.setProperty("pravega.client.auth.method", "Basic");
        System.setProperty("pravega.client.auth.token", base66EncodedToken);
    }

    private void unsetClientAuthProperties()  {
        System.clearProperty("pravega.client.auth.method");
        System.clearProperty("pravega.client.auth.token");
    }
}