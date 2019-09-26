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

import io.pravega.auth.TokenExpiredException;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.integration.demo.ClusterWrapper;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * As the package + name might suggest, this class is intended to hold integration tests for verifying delegation token
 * functionality.
 */
@Slf4j
public class DelegationTokenTest {

    /*
     * Note: No normal happy path integration tests were added here, as those scenarios are already covered in
     * other tests elsewhere (using the default token TTL and valid auth credentials).
     */

    @Test(timeout = 20000)
    public void testWriteSucceedsWhenTokenTtlIsMinusOne() throws ExecutionException, InterruptedException {
        // A Token TTL -1 indicates to the Controller to not set any expiry on the delegation token.
        writeAnEvent(-1);
    }

    @Test(timeout = 30000)
    public void testWriteFailsWhenTokenExpires() {
        // To ensure the token is certainly expired when it reaches the segment store, we are setting Controller TTL
        // as 0, so that the token expiry is set as the same time as the time it is issued in the Controller.
        assertThrows("Token expiration didn't cause write failure.",
                () -> writeAnEvent(0),
                e -> e instanceof TokenExpiredException);
    }

    @Ignore
    @Test
    public void testScenario1SharedEventWriter() throws ExecutionException, InterruptedException {
        ClusterWrapper pravegaCluster = new ClusterWrapper(true, 1);
        try {
            pravegaCluster.initialize();

            String scope = "testscope";
            String streamName = "teststream";
            int numSegments = 2;
            String message = "test message";

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            @Cleanup
            StreamManager streamManager = StreamManager.create(clientConfig);
            assertNotNull(streamManager);
            log.debug("Done creating stream manager.");

            boolean isScopeCreated = streamManager.createScope(scope);
            assertTrue("Failed to create scope", isScopeCreated);
            log.debug("Done creating stream manager.");

            boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(numSegments))
                    .build());
            Assert.assertTrue("Failed to create the stream ", isStreamCreated);

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            //@Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            log.error("Done creating the writer");

            // Note: A TokenException is thrown here if token verification fails on the server.
            try {
                for (int i=0; i< 10; i++) {
                    String evtMsg = String.format("message: %s", i);
                    writer.writeEvent("message ").get();
                    log.error("Done writing event with message [{}]", evtMsg);
                    Thread.sleep(2000);
                }
            } catch(Exception e) {
                log.error("**************", e);
                throw e;
            }
        } finally {
            pravegaCluster.close();
        }
    }

    @Ignore
    @Test
    public void testScenario2DedicatedEventWriter() throws ExecutionException, InterruptedException {
        ClusterWrapper pravegaCluster = new ClusterWrapper(true, 2);
        try {
            pravegaCluster.initialize();

            String scope = "testscope";
            String streamName = "teststream";
            int numSegments = 1;
            String message = "test message";

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            @Cleanup
            StreamManager streamManager = StreamManager.create(clientConfig);
            assertNotNull(streamManager);
            log.debug("Done creating stream manager.");

            boolean isScopeCreated = streamManager.createScope(scope);
            assertTrue("Failed to create scope", isScopeCreated);
            log.debug("Done creating stream manager.");

            boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(numSegments))
                    .build());
            Assert.assertTrue("Failed to create the stream ", isStreamCreated);

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            for (int i=0; i < 10; i++) {
                @Cleanup
                EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build());
                log.error("Done creating the writer");

                String evtMsg = String.format("message: %s", i);
                writer.writeEvent("message ").get();
                log.error("Done writing event with message [{}]", evtMsg);
                Thread.sleep(2000);
            }
        } finally {
            pravegaCluster.close();
        }
    }

    private void writeAnEvent(int tokenTtlInSeconds) throws ExecutionException, InterruptedException {
        ClusterWrapper pravegaCluster = new ClusterWrapper(true, tokenTtlInSeconds);
        try {
            pravegaCluster.initialize();

            String scope = "testscope";
            String streamName = "teststream";
            int numSegments = 1;
            String message = "test message";

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            @Cleanup
            StreamManager streamManager = StreamManager.create(clientConfig);
            assertNotNull(streamManager);
            log.debug("Done creating stream manager.");

            boolean isScopeCreated = streamManager.createScope(scope);
            assertTrue("Failed to create scope", isScopeCreated);
            log.debug("Done creating stream manager.");

            boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(numSegments))
                    .build());
            Assert.assertTrue("Failed to create the stream ", isStreamCreated);

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            //@Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

            // Note: A TokenException is thrown here if token verification fails on the server.
            writer.writeEvent(message).get();

            log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);
        } finally {
            pravegaCluster.close();
        }
    }

    @Test
    public void expiredDelegationTokenTest() throws ExecutionException, InterruptedException {
        ClusterWrapper pravegaCluster = new ClusterWrapper(true, 2);
        try {
            pravegaCluster.initialize();

            String scope = "testscope";
            String streamName = "teststream";
            int numSegments = 1;
            String message = "test message";

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");


            // Write an event

            @Cleanup
            StreamManager streamManager = StreamManager.create(clientConfig);
            assertNotNull(streamManager);
            log.debug("Done creating stream manager.");

            boolean isScopeCreated = streamManager.createScope(scope);
            assertTrue("Failed to create scope", isScopeCreated);
            log.debug("Done creating stream manager.");

            boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(numSegments))
                    .build());
            Assert.assertTrue("Failed to create the stream ", isStreamCreated);

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            //@Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

            // Note: A TokenException is thrown here if token verification fails on the server.
            writer.writeEvent(message).get();

            log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);


            // Now, read the event from the stream.

            String readerGroup = UUID.randomUUID().toString().replace("-", "");
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .disableAutomaticCheckpoints()
                    .build();

            @Cleanup
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(
                    "readerId", readerGroup,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());


            EventRead<String> event = null;

            while(true) {
                event = reader.readNextEvent(2000);
                if (event.getEvent() != null) {
                    System.out.format("Read event '%s'%n", event.getEvent());
                }
            }
        } finally {
            pravegaCluster.close();
        }

    }
}
