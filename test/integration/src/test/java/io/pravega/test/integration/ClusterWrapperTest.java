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

import static org.junit.Assert.*;

@Slf4j
public class ClusterWrapperTest {

    private static final int TIMEOUT_MILLIS = 50000;

    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

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
        String pathToConfig = "../../config/";
        String password = "secret-password";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("writer", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:testScope,READ",
                "prn::/scope:testScope/stream:testStream,READ",
                "prn::/scope:testScope/reader-group:testReaderGroup,READ"
        ));

        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tlsEnabled(true)
                .tlsServerCertificatePath(pathToConfig + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .tlsServerKeyPath(pathToConfig + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, password))
                .tlsHostVerificationEnabled(false)
                .build();

        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(pathToConfig + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "writer"))
                .build();

        cluster.initialize();

        TestUtils.createStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, testMessage, writerClientConfig);

        final ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(pathToConfig + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "reader"))
                .build();
        String readMessage = TestUtils.readAMessageFromStream(scopeName, streamName, writerClientConfig, readerGroupName);
        assertEquals(testMessage, readMessage);
    }
}
