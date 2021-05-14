/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

@Slf4j
public class EndToEndUpdateTest extends ThreadPooledTestSuite {

    private final String serviceHost = "localhost";
    private final int containerCount = 4;
    private int controllerPort;
    private URI controllerURI;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        controllerPort = TestUtils.getAvailableListenPort();
        controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
        int servicePort = TestUtils.getAvailableListenPort();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
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
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 30000)
    public void testUpdateStream() throws InterruptedException, ExecutionException, TimeoutException,
                                        TruncatedDataException, ReinitializationRequiredException {
        String scope = "scope";
        String streamName = "updateStream";

        LocalController controller = (LocalController) controllerWrapper.getController();
        controller.createScope(scope).join();
        controller.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build()).join();

        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is updated and also scaled.
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 2);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is not scaled as min num of segments is still satisfied
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 2);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 3))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 3 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 3);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.fixed(6))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 6 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 6);

        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.fixed(5))
                                    .build();
        controller.updateStream(scope, streamName, config).get();
        
        // verify that stream is scaled to have 3 segments
        assertEquals(controller.getCurrentSegments(scope, streamName).join().getNumberOfSegments(), 5);
    }
}
