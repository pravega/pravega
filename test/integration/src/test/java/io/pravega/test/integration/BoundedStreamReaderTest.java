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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@Slf4j
public class BoundedStreamReaderTest {

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
    private final Function<Integer, String> getEventData = eventNumber -> String.valueOf(eventNumber) + ":constant data"; //event
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
        executor.shutdown();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 60000)
    public void testBoundedStreamTest() throws Exception {
        createScope(SCOPE);
        createStream(STREAM1);
        createStream(STREAM2);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(1)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(2)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(3)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(4)).get();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        ReaderGroup readerGroup = groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM1),
                        //startStreamCut points to the current HEAD of stream
                        StreamCut.UNBOUNDED,
                        //endStreamCut points to the offset after two events.(i.e 2 * 30(event size) = 60)
                        new StreamCutImpl(Stream.of(SCOPE, STREAM1), ImmutableMap.of(new Segment(SCOPE, STREAM1, 0), 60L)))
                .stream(Stream.of(SCOPE, STREAM2))
                .build());

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //2. Verify if endStreamCut configuration is enforced.
        readAndVerify(reader, 1, 2);
        //The following read should not return events 3, 4 due to the endStreamCut configuration.
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

        //3. Write events to the STREAM2.
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM2, serializer,
                EventWriterConfig.builder().build());
        writer2.writeEvent(keyGenerator.get(), getEventData.apply(5)).get();
        writer2.writeEvent(keyGenerator.get(), getEventData.apply(6)).get();

        //4. Verify that events can be read from STREAM2. (Events from STREAM1 are not read since endStreamCut is reached).
        readAndVerify(reader, 5, 6);
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

    }

    private void readAndVerify(final EventStreamReader<String> reader, int...index) throws ReinitializationRequiredException {
        ArrayList<String> results = new ArrayList<>(index.length);
        for (int i = 0; i < index.length; i++) {
            String event = reader.readNextEvent(15000).getEvent();
            while (event == null) { //try until a non null event is read.
                event = reader.readNextEvent(15000).getEvent();
            }
            results.add(event);
        }
        Arrays.stream(index).forEach(i -> assertTrue(results.contains(getEventData.apply(i))));
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
