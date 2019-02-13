/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import io.pravega.common.Timer;
import lombok.Cleanup;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for TLS enabled standalone cluster.
 */
@Slf4j
public class TlsEnabledInProcPravegaClusterTest extends InProcPravegaClusterTest {

    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = false;
        this.tlsEnabled = true;
        super.setUp();
    }

    // Note: Strictly speaking, this test is really an "integration test" and is a little
    // time consuming. For now, its intended to run as a unit test, but it could be moved
    // to an integration test suite if and when necessary.
    @Test(timeout = 50000)
    public void testWriteAndReadEventWhenConfigurationIsProper() throws ExecutionException,
            InterruptedException, ReinitializationRequiredException {

        String scope = "TlsTestScope";
        String streamName = "TlsTestStream";
        int numSegments = 10;
        String message = "Test event over TLS channel";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .trustStore("../config/cert.pem")
                .validateHostName(false)
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Write an event to the stream.

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
        writer.writeEvent(message).get();
        log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);

        // Now, read the event from the stream.

        String readerGroup = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(
                    "readerId", readerGroup,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        long readTimeOut = 10000;

        EventRead<String> event = reader.readNextEvent(readTimeOut);
        if (event.isCheckpoint()) {
            log.info("Encountered a checkpoint event. Reading next event again.");
            event = reader.readNextEvent(readTimeOut);
        }

        String readMessage = event.getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
