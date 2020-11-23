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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ReadWithReadPermissionsTest {

    private static final int TIMEOUT_MILLIS = 60000;

    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

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

        // Setup the cluster and create the objects
        @Cleanup
        final ClusterWrapper cluster = new ClusterWrapper(true, "secret",
                600, writeToInternalStreamsWithReadPermission,
                this.preparePasswordInputFileEntries(passwordInputFileEntries), 4);
        cluster.initialize();
        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "creator"))
                .build();
        this.createStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        writeDataToStream(scopeName, streamName, message, writerClientConfig);

        // Now, read data back using the reader account.

        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "reader"))
                .build();
        @Cleanup
        EventStreamClientFactory readerClientFactory = EventStreamClientFactory.withScope(scopeName, readerClientConfig);
        log.debug("Created the readerClientFactory");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .disableAutomaticCheckpoints()
                .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, readerClientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        log.debug("Created reader group with name {}", readerGroupName);

        @Cleanup
        EventStreamReader<String> reader = readerClientFactory.createReader(
                "readerId", readerGroupName,
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());
        log.debug("Created an event reader");

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readMessage = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }

    private void writeDataToStream(String scope1, String stream1, String message1, ClientConfig writerClientConfig) throws InterruptedException, ExecutionException {
        @Cleanup final EventStreamClientFactory writerClientFactory = EventStreamClientFactory.withScope(scope1,
                writerClientConfig);

        @Cleanup final EventStreamWriter<String> writer = writerClientFactory.createEventWriter(stream1,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent(message1).get();
        log.info("Wrote a message to the stream {}/{}", scope1, stream1);
    }

    private List<PasswordAuthHandlerInput.Entry> preparePasswordInputFileEntries(Map<String, String> entries) {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");
            List<PasswordAuthHandlerInput.Entry> result = new ArrayList<>();
            entries.forEach((k, v) -> result.add(PasswordAuthHandlerInput.Entry.of(k, encryptedPassword, v)));
            return result;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private void createStreams(ClientConfig clientConfig, String scopeName, List<String> streamNames) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scopeName);
        assertTrue("Failed to create scope", isScopeCreated);

        streamNames.forEach(s -> {
            boolean isStreamCreated =
                    streamManager.createStream(scopeName, s, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
            if (!isStreamCreated) {
                throw new RuntimeException("Failed to create stream: " + s);
            }
        });
    }
}
