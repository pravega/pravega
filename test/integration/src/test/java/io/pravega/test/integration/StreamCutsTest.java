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

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamCutsTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
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

    @Test(timeout = 40000)
    public void testReaderGroupCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "fpj was here").get();
        writer.writeEvent("0", "fpj was here again").get();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("cuts", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream("test/test").build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("cuts");
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "cuts", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("fpj was here", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("fpj was here again", secondEvent.getEvent());

        Map<Stream, StreamCut> cuts = readerGroup.getStreamCuts();
        validateCuts(readerGroup, cuts, Collections.singleton(getQualifiedStreamSegmentName("test", "test", 0L)));

        // Scale the stream to verify that we get more segments in the cut.
        Stream stream = Stream.of("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 1st scaling");
        writer.writeEvent("0", "fpj was here again").get();
        writer.writeEvent("1", "fpj was here again").get();

        reader.readNextEvent(15000);
        cuts = readerGroup.getStreamCuts();
        HashSet<String> segmentNames = new HashSet<>();
        long one = computeSegmentId(1, 1);
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", one));
        long two = computeSegmentId(2, 1);
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", two));
        validateCuts(readerGroup, cuts, Collections.unmodifiableSet(segmentNames));

        // Scale down to verify that the number drops back.
        map = new HashMap<>();
        map.put(0.0, 1.0);
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(one);
        toSeal.add(two);
        result = controller.scaleStream(stream, Collections.unmodifiableList(toSeal), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 2nd scaling");
        writer.writeEvent("0", "fpj was here again").get();

        reader.readNextEvent(15000);
        reader.readNextEvent(15000);

        cuts = readerGroup.getStreamCuts();
        long three = computeSegmentId(3, 2);
        validateCuts(readerGroup, cuts, Collections.singleton(getQualifiedStreamSegmentName("test", "test", three)));

        // Scale up to 4 segments again.
        map = new HashMap<>();
        map.put(0.0, 0.25);
        map.put(0.25, 0.5);
        map.put(0.5, 0.75);
        map.put(0.75, 1.0);
        result = controller.scaleStream(stream, Collections.singletonList(three), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 3rd scaling");
        writer.writeEvent("0", "fpj was here again").get();

        reader.readNextEvent(15000);

        cuts = readerGroup.getStreamCuts();
        segmentNames = new HashSet<>();
        long four = computeSegmentId(4, 3);
        long five = computeSegmentId(5, 3);
        long six = computeSegmentId(6, 3);
        long seven = computeSegmentId(7, 3);
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", four));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", five));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", six));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", seven));
        validateCuts(readerGroup, cuts, Collections.unmodifiableSet(segmentNames));
    }

    private void validateCuts(ReaderGroup group, Map<Stream, StreamCut> cuts, Set<String> segmentNames) {
        Set<String> streamNames = group.getStreamNames();
        cuts.forEach((s, c) -> {
                assertTrue(streamNames.contains(s.getScopedName()));
                assertTrue(((StreamCutImpl) c).validate(segmentNames));
        });
    }
}
