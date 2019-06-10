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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
public class ControllerWatermarkingTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();

    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;
    private StreamSegmentStore store;
    private TableStore tableStore;

    @Before
    public void setup() throws Exception {
        final String serviceHost = "localhost";
        final int containerCount = 4;

        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        server.startListening();

        // 2. Start controller
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);

        controllerWrapper.awaitRunning();
    }

    @After
    public void cleanup() throws Exception {
        if (controllerWrapper != null) {
            controllerWrapper.close();
        }
        if (server != null) {
            server.close();
        }
        if (serviceBuilder != null) {
            serviceBuilder.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test(timeout = 60000)
    public void watermarkTest() throws Exception {
        Controller controller = controllerWrapper.getController();
        String scope = "scope";
        String stream = "stream";
        controller.createScope(scope).join();
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        controller.createStream(scope, stream, config).join();
        controller.createStream(scope, StreamSegmentNameUtils.getMarkSegmentForStream(stream), config).join();
        Stream streamObj = new StreamImpl(scope, stream);
        Position pos1 = new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0L), 10L));
        Position pos2 = new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0L), 20L));
        
        controller.noteTimestampFromWriter("1", streamObj, 1L, pos1).join();
        controller.noteTimestampFromWriter("2", streamObj, 2L, pos2).join();

        String markStream = StreamSegmentNameUtils.getMarkSegmentForStream(stream);
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
         ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
         ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory);
        String readerGroup = "rg";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(Stream.of(scope, markStream)).build());

        // read from the watermark segment stream using regular reader
        final EventStreamReader<Watermark> reader = clientFactory.createReader("myreader",
                readerGroup,
                new WatermarkSerializer(),
                ReaderConfig.builder().build());

        EventRead<Watermark> watermark = reader.readNextEvent(30000L);
        if (watermark.getEvent() == null) {
            // we are getting first event as null for some reason
            watermark = reader.readNextEvent(30000L);
        }
        assertNotNull(watermark.getEvent());
        assertEquals(watermark.getEvent().getLowerTimeBound(), 1L);
        assertTrue(watermark.getEvent().getStreamCut().entrySet().stream().anyMatch(x -> x.getKey().getSegmentId() == 0L && x.getValue() ==  20L));
    }
}
