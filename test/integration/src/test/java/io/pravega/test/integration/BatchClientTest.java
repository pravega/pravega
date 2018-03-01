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

import com.google.common.collect.Lists;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class BatchClientTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testBatchStream";
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Random random = new Random();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private JavaSerializer<String> serializer;

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
        serializer = new JavaSerializer<>();
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
    public void testBatchClient() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(SCOPE)
                                                        .streamName(STREAM)
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();

        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(config).get();

        //create reader and writer.
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().build(), Collections
                .singleton(STREAM));
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, serializer,
                EventWriterConfig.builder().build());

        // write events to stream with 1 segment.
        writeEvents(writer);

        // scale up and write events.
        Stream stream = new StreamImpl(SCOPE, STREAM);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue("Scale up operation", result);
        writeEvents(writer);

        //scale down and write events.
        map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        result = controller.scaleStream(stream, Arrays.asList(1, 2, 3), map, executor).getFuture().get();
        assertTrue("Scale down operation result", result);
        writeEvents(writer);

        BatchClient batchClient = clientFactory.createBatchClient();

        //List out all the segments in the stream.
        ArrayList<SegmentRange> segments = Lists.newArrayList(batchClient.getSegments(stream, null, null).getSegmentRangeIterator());
        assertEquals("Expected number of segments", 6, segments.size());

        //Batch read all events from stream.
        List<String> batchEventList = new ArrayList<>();
        segments.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, serializer);
            batchEventList.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count", 9, batchEventList.size());

        //read from a given offset.
        Segment seg0 = new Segment(SCOPE, STREAM, 0);
        SegmentRange seg0Info = SegmentRangeImpl.builder().segment(seg0).startOffset(60).endOffset(90).build();
        @Cleanup
        SegmentIterator<String> seg0Iterator = batchClient.readSegment(seg0Info, serializer);
        ArrayList<String> dataAtOffset = Lists.newArrayList(seg0Iterator);
        assertEquals(1, dataAtOffset.size());
        assertEquals(DATA_OF_SIZE_30, dataAtOffset.get(0));
    }

    private void writeEvents(EventStreamWriter<String> writer) throws InterruptedException, java.util.concurrent.ExecutionException {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).get();
        writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).get();
        writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).get();
    }
}
