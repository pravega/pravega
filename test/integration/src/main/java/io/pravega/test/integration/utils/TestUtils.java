/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.utils;

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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A helper class with general-purpose utility methods for integration tests.
 */
@Slf4j
public class TestUtils {

    public static void createStreams(ClientConfig clientConfig, String scopeName, List<String> streamNames) {
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

    @SneakyThrows
    public static void writeDataToStream(String scope1, String stream1, String message1, ClientConfig writerClientConfig) {
        @Cleanup final EventStreamClientFactory writerClientFactory = EventStreamClientFactory.withScope(scope1,
                writerClientConfig);

        @Cleanup final EventStreamWriter<String> writer = writerClientFactory.createEventWriter(stream1,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent(message1).get();
        log.info("Wrote a message to the stream {}/{}", scope1, stream1);
    }

    public static String readAMessageFromStream(String scopeName, String streamName, ClientConfig readerClientConfig,
                                                 String readerGroupName) {
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
        return readMessage;
    }

    public static List<PasswordAuthHandlerInput.Entry> preparePasswordInputFileEntries(
            Map<String, String> entries, String password) {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword(password);
            List<PasswordAuthHandlerInput.Entry> result = new ArrayList<>();
            entries.forEach((k, v) -> result.add(PasswordAuthHandlerInput.Entry.of(k, encryptedPassword, v)));
            return result;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }
}
