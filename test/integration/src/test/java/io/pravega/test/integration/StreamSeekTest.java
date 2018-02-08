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
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCut;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Slf4j
public class StreamSeekTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM1 = "testStreamSeek1";
    private static final String STREAM2 = "testStreamSeek2";
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Serializer<String> serializer = new JavaSerializer<>();
    private final Random random = new Random();
    private final Supplier<String> keyGenerator = () -> String.valueOf(random.nextInt());
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private ScheduledExecutorService executorChkpoint;

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
        executor.shutdown();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 50000)
    public void testStreamSeek() throws Exception {

        createScope(SCOPE);
        createStream(STREAM1);
        createStream(STREAM2);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                EventWriterConfig.builder().build());
        //        @Cleanup
        //        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM2, serializer,
        //                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        ReaderGroup readerGroup = groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().build(), java.util.stream.Stream.of(STREAM1, STREAM2)
                                                                                         .collect(Collectors.toSet()));

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        Function<Integer, String> getEvent = eventNumber -> String.valueOf(eventNumber) + ":constant data"; //event
        // size is 30
        writer1.writeEvent("0", getEvent.apply(1)).get();
        writer1.writeEvent("0", getEvent.apply(2)).get();

        //Offset of a streamCut is always set to zero.(Bug?)
        Map<Stream, StreamCut> sc1 = readerGroup.getStreamCuts();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        assertEquals(getEvent.apply(1), firstEvent.getEvent());

        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertEquals(getEvent.apply(2), secondEvent.getEvent());

        readerGroup.resetReadersToStreamCut(sc1.values()); //reset the readers to offset 0.
        try {
            EventRead<String> event = reader.readNextEvent(15000);
            Assert.fail("Reinitialization Exception excepted");
        } catch (ReinitializationRequiredException e) {
            reader.close();
        }

        @Cleanup
        EventStreamReader<String> readerReinitialized = clientFactory.createReader("readerId", "group", new
                        JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> event = readerReinitialized.readNextEvent(15000);
        assertEquals(getEvent.apply(1), event.getEvent());
    }

    private Checkpoint generateCheckPoint(final ReaderGroup readerGroup, final EventStreamReader<String> reader)
            throws Exception {
        String chkPointName = keyGenerator.get();
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint(chkPointName, executor);
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals(chkPointName, chkpointEvent.getCheckpointName());
        return chkPointResult.get();
    }

    private void createScope(final String scopeName) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope(scopeName).get();
    }

    private void createStream(String streamName) throws Exception {
        Controller controller = controllerWrapper.getController();
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(SCOPE)
                                                        .streamName(streamName)
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        controller.createStream(config).get();
    }
}
