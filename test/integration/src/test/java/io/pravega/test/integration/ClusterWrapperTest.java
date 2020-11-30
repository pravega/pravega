/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.test.integration.utils.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ClusterWrapperTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(50);

    @Test
    public void setsDefaultValuesWhenBuilderSpecifiesNoValues() {
        ClusterWrapper objectUnderTest = ClusterWrapper.builder().build();

        assertFalse(objectUnderTest.isAuthEnabled());
        assertTrue(objectUnderTest.isRgWritesWithReadPermEnabled());
        assertEquals(600, objectUnderTest.getTokenTtlInSeconds());
        assertEquals(4, objectUnderTest.getContainerCount());
        assertTrue(objectUnderTest.getTokenSigningKeyBasis().length() > 0);
    }

    @Test
    public void writeAndReadBackAMessageWithTlsAndAuthEnabledServer() {
        String scopeName = "testScope";
        String streamName = "testStream";
        String readerGroupName = "testReaderGroup";
        String testMessage = "test message";
        String password = "secret-password";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("writer", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:testScope,READ",
                "prn::/scope:testScope/stream:testStream,READ",
                "prn::/scope:testScope/reader-group:testReaderGroup,READ"
        ));

        // Instantiate and run the cluster
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tlsEnabled(true)
                .tlsServerCertificatePath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .tlsServerKeyPath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, password))
                .tlsHostVerificationEnabled(false)
                .build();
        cluster.initialize();

        // Write an event to the stream
        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "writer"))
                .build();
        TestUtils.createScopeAndStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, testMessage, writerClientConfig);

        // Read back the event from the stream and verify it is the same as what was written
        final ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "reader"))
                .build();
        String readMessage = TestUtils.readNextEventMessage(scopeName, streamName, readerClientConfig, readerGroupName);
        assertEquals(testMessage, readMessage);
    }
}
