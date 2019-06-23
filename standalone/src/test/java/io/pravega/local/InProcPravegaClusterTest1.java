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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.net.ServerSocket;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class contains tests for in-process standalone cluster.
 */
@Slf4j
public class InProcPravegaClusterTest1 extends InProcPravegaClusterTest {

    private  String message = "testmessage";
    private  String scope = "testscope";
    private  String streamName = "teststream";
    private  URI controllerUri = null;
    @Before
    @Override
    public void setUp() throws Exception {
        log.info("stand alone running: {}", standaloneRunning()); 
        if (!standaloneRunning()) {
            this.authEnabled = false;
            this.tlsEnabled = false;
            this.restEnabled = true;
            super.setUp();
            controllerUri = URI.create(localPravega.getInProcPravegaCluster().getControllerURI());
        } else {
            controllerUri = URI.create("tcp://localhost:9090");
        }
    }

    boolean standaloneRunning() {
        try {    
            ServerSocket serverSocket = new ServerSocket(9090);
            serverSocket.close();
            } catch (IOException e) {
                log.info("port is in use");
                return true;
            }
            return false;
    }

    /**
     * Compares reads and writes to verify that an in-process Pravega cluster responds properly with
     * with valid client configuration.
     *
     * Note:
     * Strictly speaking, this test is really an "integration test" and is a little time consuming. For now, its
     * intended to also run as a unit test, but it could be moved to an integration test suite if and when necessary.
     *
     */
    @Test(timeout = 50000)
    public void testWriteAndReadEventWithValidClientConfig() throws ExecutionException,
            InterruptedException, ReinitializationRequiredException {

        int numSegments = 1;

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build(); 

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
        String readMessage = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }


    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
