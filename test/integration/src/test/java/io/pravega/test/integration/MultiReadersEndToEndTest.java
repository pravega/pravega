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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.integration.utils.IntegerSerializer;
import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.test.common.TestUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class MultiReadersEndToEndTest {

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    // The number of events to generate for each test.
    private static final int NUM_TEST_EVENTS = 1000;

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test(timeout = 30000)
    public void testReadersEqualToSegments() throws Exception {
        runTest(Collections.singleton("teststream1"), 2, 2);
        runTestUsingMock(Collections.singleton("teststream1"), 2, 2);
    }
    
    @Test(timeout = 30000)  
     public void testReadersLessThanSegments() throws Exception {      
        runTest(Collections.singleton("teststream2"), 2, 3);
        runTestUsingMock(Collections.singleton("teststream2"), 2, 3);
    }
    
    @Test(timeout = 30000)
    public void testReadersGreaterThanSegments() throws Exception {
        runTest(Collections.singleton("teststream3"), 3, 2);
        runTestUsingMock(Collections.singleton("teststream3"), 3, 2);
    }

    @Test(timeout = 30000)
    public void testMultiStreams() throws Exception {
        Set<String> testStreams = new HashSet<>();
        testStreams.add("teststream4");
        testStreams.add("teststream5");
        runTest(testStreams, 2, 2);
        runTestUsingMock(testStreams, 2, 2);
    }

    private void runTest(final Set<String> streamNames, final int numParallelReaders, final int numSegments)
            throws Exception {
        @Cleanup
        StreamManager streamManager = StreamManager.create(SETUP_UTILS.getControllerUri());
        streamManager.createScope(SETUP_UTILS.getScope());
        streamNames.stream().forEach(stream -> {
            streamManager.createStream(SETUP_UTILS.getScope(),
                                       stream,
                                       StreamConfiguration.builder()
                                               .scope(SETUP_UTILS.getScope())
                                               .streamName(stream)
                                               .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                               .build());
            log.info("Created stream: {}", stream);
        });

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getControllerUri());
        streamNames.stream().forEach(stream -> {
            EventStreamWriter<Integer> eventWriter = clientFactory.createEventWriter(
                    stream, new IntegerSerializer(), EventWriterConfig.builder().build());
            for (Integer i = 0; i < NUM_TEST_EVENTS; i++) {
                eventWriter.writeEvent(String.valueOf(i), i);
            }
            eventWriter.flush();
            log.info("Wrote {} events", NUM_TEST_EVENTS);
        });

        final String readerGroupName = "testreadergroup" + RandomStringUtils.randomAlphanumeric(10).toLowerCase();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(),
                                                                             SETUP_UTILS.getControllerUri());
        readerGroupManager.createReaderGroup(readerGroupName,
                                             ReaderGroupConfig.builder().startingTime(0).build(),
                                             streamNames);

        Collection<Integer> read = readAllEvents(numParallelReaders, clientFactory, readerGroupName, numSegments);

        Assert.assertEquals(NUM_TEST_EVENTS * streamNames.size(), read.size());
        // Check unique events.
        Assert.assertEquals(NUM_TEST_EVENTS, new TreeSet<>(read).size());
    }

    private Collection<Integer> readAllEvents(final int numParallelReaders, ClientFactory clientFactory,
                                              final String readerGroupName, final int numSegments) {
        ConcurrentLinkedQueue<Integer> read = new ConcurrentLinkedQueue<>();
        final ExecutorService executorService = Executors.newFixedThreadPool(
                numParallelReaders, new ThreadFactoryBuilder().setNameFormat("testreader-pool-%d").build());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numParallelReaders; i++) {
            futures.add(executorService.submit(() -> {
                final String readerId = UUID.randomUUID().toString();
                @Cleanup
                final EventStreamReader<Integer> reader = clientFactory.createReader(readerId,
                                                                                     readerGroupName,
                                                                                     new IntegerSerializer(),
                                                                                     ReaderConfig.builder().build());
                int emptyCount = 0;
                while (emptyCount <= numSegments) {
                    try {
                        final Integer integerEventRead = reader.readNextEvent(1000).getEvent();
                        if (integerEventRead != null) {
                            read.add(integerEventRead);
                            emptyCount = 0;
                        } else {
                            emptyCount++;
                        }
                    } catch (ReinitializationRequiredException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }

        // Wait until all readers are done.
        futures.forEach(f -> FutureHelpers.getAndHandleExceptions(f, RuntimeException::new));
        executorService.shutdownNow();
        return read;
    }
    
    private void runTestUsingMock(final Set<String> streamNames, final int numParallelReaders, final int numSegments)
            throws Exception {
        int servicePort = TestUtils.getAvailableListenPort();
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", "localhost", servicePort);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("scope");
        streamNames.stream().forEach(stream -> {
            streamManager.createStream("scope",
                                       stream,
                                       StreamConfiguration.builder()
                                       .scope("scope")
                                       .streamName(stream)
                                       .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                       .build());
            EventStreamWriter<Integer> eventWriter = clientFactory.createEventWriter(stream,
                                                                                     new IntegerSerializer(),
                                                                                     EventWriterConfig.builder()
                                                                                     .build());
            for (Integer i = 0; i < NUM_TEST_EVENTS; i++) {
                eventWriter.writeEvent(String.valueOf(i), i);
            }
            eventWriter.flush();
            log.info("Wrote {} events", NUM_TEST_EVENTS);
        });

        final String readerGroupName = "testReaderGroup";
        streamManager.createReaderGroup(readerGroupName,
                                        ReaderGroupConfig.builder().startingTime(0).build(),
                                        streamNames);

        Collection<Integer> read = readAllEvents(numParallelReaders, clientFactory, readerGroupName, numSegments);

        Assert.assertEquals(NUM_TEST_EVENTS * streamNames.size(), read.size());
        // Check unique events.
        Assert.assertEquals(NUM_TEST_EVENTS, new TreeSet<>(read).size());
    }
}
