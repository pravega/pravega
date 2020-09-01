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

    @Test
    public void readUsingUserAccountsWithReadPermission() throws ExecutionException, InterruptedException {
        // Writing data so that we can check the read flow
        final String scopeName = "MarketData";
        final String streamName = "StockPriceUpdates";
        final String readerGroupName = "PriceChangeCalculator";
        final String message = "SCRIP:DELL,EXCHANGE:NYSE,PRICE=100";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ_UPDATE",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ_UPDATE",
                "prn::/scope:MarketData/stream:_RGPriceChangeCalculator,READ_UPDATE",
                "prn::/scope:MarketData/stream:_MARKStockPriceUpdates,READ_UPDATE"
        ));
        log.debug("passwordInputFileEntries prepared: {}", passwordInputFileEntries);

        @Cleanup
        final ClusterWrapper cluster = new ClusterWrapper(true, "secret",
                600, this.preparePasswordInputFileEntries(passwordInputFileEntries), 4);
        cluster.initialize();
        log.debug("Cluster initialized successfully");

        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "creator"))
                .build();
        this.createStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        log.debug("Streams created");

        writeDataToStream(scopeName, streamName, message, writerClientConfig);

        // Reading data now

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

    @SneakyThrows
    @Test
    public void readerGroupReadsStreamsInAnotherScope() {

        // Writing data so that we can check the read flow
        final String scope1 = "scope1";
        final String stream1 = "stream1";
        final String scope2 = "scope2";
        final String stream2 = "stream2";
        final String scope3 = "scope3"; // We'll use this to house the reader group.
        final String message1 = "message 1 in scope1/stream1";
        final String message2 = "message 2 in scope2/stream2";
        final String readerGroupName = "readerGroup";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", "prn::*,READ_UPDATE");
        /*passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:scope1,READ_UPDATE",
                "prn::/scope:scope1/stream:stream1,READ_UPDATE",
                "prn::/scope:scope1/stream:_RGreaderGroup,READ_UPDATE",
                "prn::/scope:scope1/stream:_MARKstream1,READ_UPDATE"
        ));*/
        log.info("passwordInputFileEntries prepared: {}", passwordInputFileEntries);

        @Cleanup
        final ClusterWrapper cluster = new ClusterWrapper(true, "secret",
                600, this.preparePasswordInputFileEntries(passwordInputFileEntries), 4);
        cluster.initialize();
        log.info("Cluster initialized successfully");

        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "creator"))
                .build();

        this.createStreams(writerClientConfig, scope1, Arrays.asList(stream1));
        this.createStreams(writerClientConfig, scope2, Arrays.asList(stream2));
        this.createScope(writerClientConfig, scope3);
        log.info("Scopes/streams created");

        writeDataToStream(scope1, stream1, message1, writerClientConfig);
        log.info("Wrote message {} to {}/{}", message1, scope1, stream1);

        writeDataToStream(scope2, stream2, message2, writerClientConfig);
        log.info("Wrote message {} to {}/{}", message2, scope2, stream2);

        // Now, do the reading

        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "reader"))
                .build();

        @Cleanup
        EventStreamClientFactory readerClientFactory = EventStreamClientFactory.withScope(scope3, readerClientConfig);
        log.info("Created the readerClientFactory");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope1, stream1))
                .stream(Stream.of(scope2, stream2))
                .disableAutomaticCheckpoints()
                .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope3, readerClientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        log.info("Created reader group with name {}", readerGroupName);

        @Cleanup
        EventStreamReader<String> reader = readerClientFactory.createReader(
                "readerId", readerGroupName,
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());
        log.info("Created an event reader");

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readMessage1 = reader.readNextEvent(10000).getEvent();
        log.info("Read message1L [{}]", readMessage1);

        String readMessage2 = reader.readNextEvent(10000).getEvent();
        log.info("Read message2: [{}]", readMessage2);
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

    private void createScope(ClientConfig clientConfig, String scopeName) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scopeName);
        assertTrue("Failed to create scope", isScopeCreated);
    }
}
