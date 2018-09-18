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
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ReaderGroupStreamCutUpdateTest {

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
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 60000)
    public void testStreamcutsUpdateInReaderGroup() throws Exception {
        final String scope = "testStreamcutsUpdateInReaderGroup";
        final String stream = "myStream";
        final String readerGroupName = "testStreamcutsUpdateInReaderGroupRG";
        final int checkpointingIntervalMs = 2000;
        final int readerSleepInterval = 250;
        final int numEvents = 100;

        // First, create the stream.
        StreamManager streamManager = StreamManager.create(controllerURI);
        Assert.assertTrue(streamManager.createScope(scope));
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scope(scope)
                                                                     .streamName(stream)
                                                                     .scalingPolicy(ScalingPolicy.fixed(2))
                                                                     .build();
        streamManager.createStream(scope, stream, streamConfiguration);

        // Write some events in the stream.
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        writeEvents(clientFactory, stream, numEvents);

        // Read the events and test that positions are getting updated.
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                               .stream(Stream.of(scope, stream))
                                                               .automaticCheckpointIntervalMillis(checkpointingIntervalMs)
                                                               .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
        @Cleanup
        EventStreamReader<Double> reader = clientFactory.createReader("myReader", readerGroupName,
                new JavaSerializer<>(), ReaderConfig.builder().build());

        Map<Stream, StreamCut> currentStreamcuts = readerGroup.getStreamCuts();
        EventRead eventRead;
        int lastIteration = 0, iteration = 0;
        int assertionFrequency = checkpointingIntervalMs / readerSleepInterval;
        do {
            eventRead = reader.readNextEvent(5000);

            // Check that the streamcuts are being updated periodically via automatic reader group checkpoints.
            if (iteration != lastIteration && iteration % assertionFrequency == 0) {
                log.info("Comparing streamcuts: {} / {} in iteration {}.", currentStreamcuts, readerGroup.getStreamCuts(), iteration);
                Assert.assertNotEquals(currentStreamcuts, readerGroup.getStreamCuts());
                currentStreamcuts = readerGroup.getStreamCuts();
                lastIteration = iteration;
            }

            Thread.sleep(readerSleepInterval);
            if (!eventRead.isCheckpoint()) {
                iteration++;
            }
        } while ((eventRead.isCheckpoint() || eventRead.getEvent() != null) && iteration < numEvents);
    }

    private void writeEvents(ClientFactory clientFactory, String streamName, int totalEvents, int offset) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = offset; i < totalEvents; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.info("Writing event: {} to stream {}", i, streamName);
        }
    }

    private void writeEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        writeEvents(clientFactory, streamName, totalEvents, 0);
    }
}
