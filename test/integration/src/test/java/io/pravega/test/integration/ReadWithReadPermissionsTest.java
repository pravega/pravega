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
import io.pravega.controller.server.security.auth.StrongPasswordProcessor;
import io.pravega.test.integration.demo.ClusterWrapper;
import io.pravega.test.integration.utils.PasswordAuthHandlerInput;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    String scopeName = "MarketData";
    String streamName = "StockPriceUpdates";
    String readerGroupName = "PriceChangeCalculator";
    String message = "SCRIP:DELL,EXCHANGE:NYSE,PRICE=100";

    ClusterWrapper cluster;

    @SneakyThrows
    @Before
    public void setup() {
        Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                    "prn::/scope:MarketData,READ_UPDATE",
                                 "prn::/scope:MarketData/stream:StockPriceUpdates,READ_UPDATE",
                                 "prn::/scope:MarketData/stream:_RGPriceChangeCalculator,READ_UPDATE",
                                 "prn::/scope:MarketData/stream:_MARKStockPriceUpdates,READ_UPDATE"
                ));
        cluster = new ClusterWrapper(true, "secret",
                600, this.preparePasswordInputFileEntries(passwordInputFileEntries), 4);
        cluster.initialize();

        ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "creator"))
                .build();
        this.createStreams(writerClientConfig, scopeName, Arrays.asList(streamName));

        @Cleanup
        EventStreamClientFactory writerClientFactory = EventStreamClientFactory.withScope(scopeName, writerClientConfig);

        @Cleanup
        EventStreamWriter<String> writer = writerClientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent(message).get();
        log.info("Done writing a message");
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void readUsingUserAccountsWithReadPermission() throws ExecutionException, InterruptedException {
        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "reader"))
                .build();
        @Cleanup
        EventStreamClientFactory readerClientFactory = EventStreamClientFactory.withScope(scopeName, readerClientConfig);

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .disableAutomaticCheckpoints()
                .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, readerClientConfig);
        readerGroupManager.createReaderGroup(this.readerGroupName, readerGroupConfig);

        @Cleanup
        EventStreamReader<String> reader = readerClientFactory.createReader(
                "readerId", readerGroupName,
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readMessage = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
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
