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

import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.test.integration.utils.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ReadWithReadPermissionsTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    /**
     * This test verifies that data can be read from a stream using read-only permissions, if the system is configured
     * to allow writes to internal streams with read-only permissions.
     */
    @SneakyThrows
    @Test
    public void readWithReadOnlyPermissions() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();

        // This `creator` account is used to create the objects
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");

        // This `reader` account is used to read back from the stream.
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ"
        ));
        writeThenReadDataBack(passwordInputFileEntries, true);
    }

    @Test
    public void readsRequireWritePermissionsOnRgWhenConfigIsFalse() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ_UPDATE",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ"
        ));
        AssertExtensions.assertThrows(StatusRuntimeException.class,
                () -> writeThenReadDataBack(passwordInputFileEntries, false));
    }

    @SneakyThrows
    @Test
    public void readsWorkWithWritePermissionsWhenConfigIsFalse() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ_UPDATE",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ_UPDATE"
        ));
        writeThenReadDataBack(passwordInputFileEntries, false);
    }

    @SneakyThrows
    private void writeThenReadDataBack(Map<String, String> passwordInputFileEntries,
                                       boolean writeToInternalStreamsWithReadPermission) {
        final String scopeName = "MarketData";
        final String streamName = "StockPriceUpdates";
        final String readerGroupName = "PriceChangeCalculator";
        final String message = "SCRIP:DELL,EXCHANGE:NYSE,PRICE=100";
        final String pwd = "secret-password";

        // Setup the cluster and create the objects
        @Cleanup
        final ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tokenSigningKeyBasis("secret").tokenTtlInSeconds(600)
                .rgWritesWithReadPermEnabled(writeToInternalStreamsWithReadPermission)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, pwd))
                .build();
        cluster.initialize();

        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(pwd, "creator"))
                .build();
        TestUtils.createScopeAndStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, message, writerClientConfig);

        // Now, read data back using the reader account.

        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(pwd, "reader"))
                .build();

        String readMessage = TestUtils.readNextEventMessage(scopeName, streamName, readerClientConfig, readerGroupName);
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }
}
