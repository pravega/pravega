/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;

import java.net.URI;

import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

/**
 * This class contains tests for in-process standalone cluster. It also configures and runs standalone mode cluster
 * with appropriate configuration for itself as well as for sub-classes.
 */
@Slf4j
public class InProcPravegaClusterTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = new PravegaEmulatorResource(false, false, false);
    final String msg = "Test message on the plaintext channel";

    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(EMULATOR.pravega.getInProcPravegaCluster().getControllerURI()))
                .build();
    }

    /**
     * Compares reads and writes to verify that an in-process Pravega cluster responds properly with
     * with valid client configuration.
     *
     * Note:
     * Strictly speaking, this test is really an "integration test" and is a little time consuming. For now, its
     * intended to also run as a unit test, but it could be moved to an integration test suite if and when necessary.
     *
     */
    @Test(timeout = 30000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent("TestScope", "TestStream", msg, prepareValidClientConfig());
    }
}
