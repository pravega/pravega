/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientFactory;
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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamRecreationTest {

    private static final int NUM_SCOPES = 3;
    private static final int NUM_STREAMS = 5;
    private static final int NUM_EVENTS = 100;
    private static final int TEST_ITERATIONS = 3;


    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    private Map<String, List<Long>> controllerPerfStats = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();

        // Performance inspection.
        controllerPerfStats.put("createScopeMs", new ArrayList<>());
        controllerPerfStats.put("createStreamMs", new ArrayList<>());
        controllerPerfStats.put("sealStreamMs", new ArrayList<>());
        controllerPerfStats.put("deleteStreamMs", new ArrayList<>());
        controllerPerfStats.put("deleteScopeMs", new ArrayList<>());
        controllerPerfStats.put("updateStreamMs", new ArrayList<>());
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testStreamRecreation() throws Exception {
        final String myScope = "myScope";
        final String myStream = "myStream";
        final String myReaderGroup = "myReaderGroup";
        final int numIterations = 10;

        // Create the scope and the stream.
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(myScope);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(myScope, controllerURI);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                                     .stream(Stream.of(myScope, myStream))
                                                                     .build();

        for (int i = 0; i < numIterations; i++) {
            log.info("Stream re-creation iteration {}.", i);
            final String eventContent = "myEvent" + String.valueOf(i);
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                         .scope(myScope)
                                                                         .streamName(myStream)
                                                                         .scalingPolicy(ScalingPolicy.fixed(i + 1))
                                                                         .build();
            streamManager.createStream(myScope, myStream, streamConfiguration);

            // Write a single event.
            @Cleanup
            ClientFactory clientFactory = ClientFactory.withScope(myScope, controllerURI);
            EventStreamWriter<String> writer = clientFactory.createEventWriter(myStream, new JavaSerializer<>(),
                    EventWriterConfig.builder().build());
            writer.writeEvent(eventContent).join();
            writer.close();

            // Read the event
            readerGroupManager.createReaderGroup(myReaderGroup, readerGroupConfig);
            readerGroupManager.getReaderGroup(myReaderGroup).resetReaderGroup(readerGroupConfig);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader("myReader", myReaderGroup, new JavaSerializer<>(),
                    ReaderConfig.builder().build());
            String readResult;
            do {
                readResult = reader.readNextEvent(1000).getEvent();
            } while (readResult == null);
            assertEquals("Wrong event read in re-created stream", eventContent, readResult);

            // Delete the stream.
            assertTrue("Unable to seal re-created stream.", streamManager.sealStream(myScope, myStream));
            assertTrue("Unable to delete re-created stream.", streamManager.deleteStream(myScope, myStream));
        }
    }

    @Test(timeout = 200000)
    public void testStreamRecreation2() throws Exception {
        // Perform management tests with Streams and Scopes.
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            log.info("Stream and scope management test in iteration {}.", i);
            testStreamScopeManagementIteration();
        }

        // Provide some performance information of Stream/Scope metadata operations.
        for (String perfKey : controllerPerfStats.keySet()) {
            log.info("Performance of {}: {}", perfKey, controllerPerfStats.get(perfKey).stream().mapToLong(x -> x).summaryStatistics());
        }

        log.debug("Scope and Stream management test passed.");
    }

    // Start region utils

    private void testStreamScopeManagementIteration() {
        for (int i = 0; i < NUM_SCOPES; i++) {
            final String scope = "testStreamsAndScopesManagement" + String.valueOf(i);
            testCreateScope(scope);
            testCreateSealAndDeleteStreams(scope);
            testDeleteScope(scope);
        }
    }

    private void testCreateScope(String scope) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        assertFalse(streamManager.deleteScope(scope));
        long iniTime = System.nanoTime();
        assertTrue("Creating scope", streamManager.createScope(scope));
        controllerPerfStats.get("createScopeMs").add(timeDiffInMs(iniTime));
    }

    private void testDeleteScope(String scope) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        assertFalse(streamManager.createScope(scope));
        long iniTime = System.nanoTime();
        assertTrue("Deleting scope", streamManager.deleteScope(scope));
        controllerPerfStats.get("deleteScopeMs").add(timeDiffInMs(iniTime));
    }

    private void testCreateSealAndDeleteStreams(String scope) {
        for (int j = 1; j <= NUM_STREAMS; j++) {
            @Cleanup
            StreamManager streamManager = StreamManager.create(controllerURI);
            final String stream = String.valueOf(j);
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j)).build();

            // Create Stream with nonexistent scope, which should not be successful.
            log.info("Creating a stream in a deliberately nonexistent scope nonexistentScope/{}.", stream);
            assertThrows(RuntimeException.class, () -> streamManager.createStream("nonexistentScope", stream,
                    StreamConfiguration.builder().build()));
            long iniTime = System.nanoTime();
            log.info("Creating stream {}/{}.", scope, stream);
            assertTrue("Creating stream", streamManager.createStream(scope, stream, config));
            controllerPerfStats.get("createStreamMs").add(timeDiffInMs(iniTime));

            // Update the configuration of the stream by doubling the number of segments.
            config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j * 2)).build();
            iniTime = System.nanoTime();
            assertTrue(streamManager.updateStream(scope, stream, config));
            controllerPerfStats.get("updateStreamMs").add(timeDiffInMs(iniTime));

            // Perform tests on empty and non-empty streams.
            if (j % 2 == 0) {
                log.info("Writing events in stream {}/{}.", scope, stream);
                @Cleanup
                ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                writeEvents(clientFactory, stream, NUM_EVENTS);
            }

            // Update the configuration of the stream.
            config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j * 2)).build();
            assertTrue(streamManager.updateStream(scope, stream, config));

            // Attempting to delete non-empty scope and non-sealed stream.
            assertThrows(RuntimeException.class, () -> streamManager.deleteScope(scope));
            assertThrows(RuntimeException.class, () -> streamManager.deleteStream(scope, stream));

            // Seal and delete stream.
            log.info("Attempting to seal and delete stream {}/{}.", scope, stream);
            iniTime = System.nanoTime();
            assertTrue(streamManager.sealStream(scope, stream));
            controllerPerfStats.get("sealStreamMs").add(timeDiffInMs(iniTime));
            iniTime = System.nanoTime();
            assertTrue(streamManager.deleteStream(scope, stream));
            controllerPerfStats.get("deleteStreamMs").add(timeDiffInMs(iniTime));

            // Seal and delete already sealed/deleted streams.
            log.info("Sealing and deleting an already deleted stream {}/{}.", scope, stream);
            assertThrows(RuntimeException.class, () -> streamManager.sealStream(scope, stream));
            assertFalse(streamManager.deleteStream(scope, stream));
        }
    }

    private void writeEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < totalEvents; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.debug("Writing event: {} to stream {}", i, streamName);
        }
    }

    private long timeDiffInMs(long iniTime) {
        return (System.nanoTime() - iniTime) / 1000000;
    }
}
