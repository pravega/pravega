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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ScopeTest {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
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
    public void testScale() throws Exception {
        final String scope = "test";
        final String streamName1 = "test1";
        final String streamName2 = "test2";
        final String streamName3 = "test3";
        final Map<String, Integer> foundCount = new HashMap<>();
        foundCount.put(streamName1, 0);
        foundCount.put(streamName2, 0);
        foundCount.put(streamName3, 0);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();

        @Cleanup
        Controller controller = controllerWrapper.getController();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());

        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName1, config).get();
        controller.createStream(scope, streamName2, config).get();
        controller.createStream(scope, streamName3, config).get();

        StreamManager manager = new StreamManagerImpl(controller, connectionFactory);

        Iterator<Stream> iterator = manager.listStreams(scope);
        assertTrue(iterator.hasNext());
        Stream next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertFalse(iterator.hasNext());

        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 1));

        AsyncIterator<Stream> asyncIterator = controller.listStreams(scope);
        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        assertNull(next);
        
        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 2));
    }
}
