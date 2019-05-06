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
import io.pravega.test.common.PasswordAuthHandlerInput;
import io.pravega.test.integration.demo.ControllerWrapper;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

/**
 * The tests in this class are intended to verify whether Batch Client works with a Pravega cluster
 * that has "Auth" (short for authentication and authorization) enabled.
 *
 * This class inherits the tests of the parent class. Some of the test methods of the parent are reproduced here to
 * make it easier to run each test interactively if needed (for debugging purposes).
 */
public class BatchClientWithAuthTest extends BatchClientTest {

    private static final File pwdAuthHandlerInput = createAuthFile();

    @AfterClass
    public static void classTearDown() {
        if (pwdAuthHandlerInput.exists()) {
            pwdAuthHandlerInput.delete();
        }
    }

    @Override
    protected ClientConfig createClientConfig () {
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
        File inputFile = createAuthFile();
        return new ControllerWrapper(zkTestServer.getConnectString(),
                false, true,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount, -1,
                true, inputFile.getPath(), "secret");
    }

    @Test
    @Override
    public void testBatchClient() throws Exception {
        super.testBatchClient();
    }

    @Test
    @Override
    public void testBatchClientWithStreamTruncation() throws Exception {
        super.testBatchClientWithStreamTruncation();
    }

    private static File createAuthFile() {
        PasswordAuthHandlerInput inputFile = new PasswordAuthHandlerInput("BatchClientAuth",
                ".txt");

        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();
        String encryptedPassword = null;
        try {
            encryptedPassword = passwordEncryptor.encryptPassword("1111_aaaa");
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
        inputFile.addEntry(PasswordAuthHandlerInput.Entry.of("admin", encryptedPassword, "*,READ_UPDATE;"));
        return inputFile.getInputFile();
    }
}
