/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import java.net.URI;
import java.util.UUID;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for auth enabled in-process standalone cluster.
 */
@Slf4j
public class AuthEnabledInProcPravegaClusterTest extends InProcPravegaClusterTest {
    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = true;
        this.tlsEnabled = false;
        super.setUp();
    }

    @Override
    public void createTestStream() {
        // This test from the base is unnecessary in this class as that and more is already being
        // tested in another test method.
    }

    /**
     * Compares reads and writes to verify that an in-process Pravega cluster runs properly with
     * with valid auth (authentication and authorization) configuration.
     *
     * Note:
     * Strictly speaking, this test is really an "integration test" and is a little time consuming. For now, its
     * intended to also run as a unit test, but it could be moved to an integration test suite if and when necessary.
     *
     */
    @Test(timeout = 50000)
    public void testWriteAndReadEventWhenConfigIsProper() throws ReinitializationRequiredException {
        String scope = "org.example.auth";
        String streamName = "stream1";
        int numSegments = 1;

        ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .build();
        log.info("Done creating client config");

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Created a stream manager");

        streamManager.createScope(scope);
        log.info("Created a scope [{}]", scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        log.info("Created a stream with name [{}]", streamName);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        log.info("Got a writer");

        String writeEvent1 = "This is event 1";
        writer.writeEvent(writeEvent1);
        log.info("Done writing event [{}]", writeEvent1);

        String writeEvent2 = "This is event 2";
        writer.writeEvent(writeEvent2);
        log.info("Done writing event [{}]", writeEvent2);

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

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readEvent1 = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readEvent1);

        String readEvent2 = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readEvent2);

        assertEquals(writeEvent1, readEvent1);
        assertEquals(writeEvent2, readEvent2);
    }

    @Test
    public void testCreateStreamFailsWhenCredentialsAreInvalid() {
        Assert.assertNotNull("Pravega not initialized", localPravega);
        String scope = "Scope";

        ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new DefaultCredentials("", ""))
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        AssertExtensions.assertThrows(RuntimeException.class,
                () -> streamManager.createScope(scope));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}