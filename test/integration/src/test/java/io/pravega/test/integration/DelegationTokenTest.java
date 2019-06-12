/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.integration.demo.ClusterWrapper;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * As the package + name might suggest, this class is intended to hold integration tests for verifying delegation token
 * functionality.
 */
@Slf4j
public class DelegationTokenTest {

    @Test(timeout = 50000)
    public void testWriteSucceedsWhenTokenIsNotExpired() throws ExecutionException, InterruptedException {
        // By keeping the ttl high (200 seconds), we are trying to ensure that that the token lives for sufficient time
        // for this test to complete while the token is still alive.
        writeAnEvent(200);
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

            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            writer.writeEvent(message).get();
            log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);
        } finally {
            pravegaCluster.close();
        }
    }
}
