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
import io.pravega.client.stream.EventRead;
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
import io.pravega.test.integration.demo.ClusterWrapper;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;

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

    /**
     * This test verifies that a event stream reader continues to read events as a result of automatic delegation token
     * renewal, after the initial delegation token it uses expires.
     *
     * We use an extraordinarily high test timeout and read time outs to account for any inordinate delays that may be
     * encountered in testing environments.
     */
    @Test(timeout = 50000)
    public void testDelegationTokenGetsRenewedAfterExpiry() throws InterruptedException {
        // Delegation token renewal threshold is 5 seconds, so we are using 6 seconds as Token TTL so that token doesn't
        // get renewed before each use.
        ClusterWrapper pravegaCluster = new ClusterWrapper(true, 6);
        try {
            pravegaCluster.initialize();

            final String scope = "testscope";
            final String streamName = "teststream";
            final int numSegments = 1;

            final ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            @Cleanup
            final StreamManager streamManager = StreamManager.create(clientConfig);
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
            final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            // Perform writes on a separate thread.
            Runnable runnable = () -> {
                @Cleanup
                EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build());

                for (int i = 0; i < 10; i++) {
                    String msg = "message: " + i;
                    writer.writeEvent(msg).join();
                    log.debug("Done writing message '{}' to stream '{} / {}'", msg, scope, streamName);
                }
            };
            Thread writerThread = new Thread(runnable);
            writerThread.start();

            // Now, read the events from the stream.

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

            int j = 0;
            EventRead<String> event = null;
            do {
                event = reader.readNextEvent(2000);
                if (event.getEvent() != null) {
                    log.info("Done reading event: {}", event.getEvent());
                    j++;
                }

                // We are keeping sleep time relatively large, just to make sure that the delegation token expires
                // midway.
                Thread.sleep(500);
            } while (event.getEvent() != null);

            // Assert that we end up reading 10 events even though delegation token must have expired midway.
            //
            // To look for evidence of delegation token renewal check the logs for the following message:
            // - "Token is nearing expiry, so refreshing it"
            assertSame(10, j);
        } finally {
            pravegaCluster.close();
        }
    }
}
