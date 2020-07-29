/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Controller stream metadata tests.
 */
@Slf4j
public class ControllerStreamMetadataTest {
    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StreamConfiguration streamConfiguration = null;

    @Before
    public void setUp() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        try {
            // 1. Start ZK
            this.zkTestServer = new TestingServerStarter().start();

            // 2. Start Pravega service.
            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            TableStore tableStore = serviceBuilder.createTableStoreService();

            this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
            this.server.startListening();

            // 3. Start controller
            this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                    controllerPort, serviceHost, servicePort, containerCount);
            this.controllerWrapper.awaitRunning();
            this.controller = controllerWrapper.getController();
            this.streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
        } catch (Exception e) {
            log.error("Error during setup", e);
            throw e;
        }
    }

    @After
    public void tearDown() {
        try {
            if (this.controllerWrapper != null) {
                this.controllerWrapper.close();
                this.controllerWrapper = null;
            }
            if (this.server != null) {
                this.server.close();
                this.server = null;
            }
            if (this.zkTestServer != null) {
                this.zkTestServer.close();
                this.zkTestServer = null;
            }
        } catch (Exception e) {
            log.warn("Exception while tearing down", e);
        }
    }

    @Test(timeout = 10000)
    public void streamMetadataTest() throws Exception {
        // Create test scope. This operation should succeed.
        assertTrue(controller.createScope(SCOPE).join());

        // Delete the test scope. This operation should also succeed.
        assertTrue(controller.deleteScope(SCOPE).join());

        // Try creating a stream. It should fail, since the scope does not exist.
        assertFalse(Futures.await(controller.createStream(SCOPE, STREAM, streamConfiguration)));

        // Again create the scope.
        assertTrue(controller.createScope(SCOPE).join());

        // Try creating the stream again. It should succeed now, since the scope exists.
        assertTrue(controller.createStream(SCOPE, STREAM, streamConfiguration).join());

        // Delete test scope. This operation should fail, since it is not empty.
        assertFalse(Futures.await(controller.deleteScope(SCOPE)));

        // Try creating already existing scope.
        assertFalse(controller.createScope(SCOPE).join());

        // Try creating already existing stream.
        assertFalse(controller.createStream(SCOPE, STREAM, streamConfiguration).join());

        // Delete test stream. This operation should fail, since it is not yet SEALED.
        assertFalse(Futures.await(controller.deleteStream(SCOPE, STREAM)));

        // Seal the test stream. This operation should succeed.
        assertTrue(controller.sealStream(SCOPE, STREAM).join());

        // Delete test stream. This operation should succeed.
        assertTrue(controller.deleteStream(SCOPE, STREAM).join());

        // Delete test stream again. Now it should fail.
        assertFalse(controller.deleteStream(SCOPE, STREAM).join());

        // Delete test scope. This operation sholud succeed.
        assertTrue(controller.deleteScope(SCOPE).join());

        // Delete a non-existent scope.
        assertFalse(controller.deleteScope("non_existent_scope").join());

        // Create a scope with invalid characters. It should fail.
        assertFalse(Futures.await(controller.createScope("abc/def")));

        // Try creating stream with invalid characters. It should fail.
        assertFalse(Futures.await(controller.createStream(SCOPE, "abc/def", StreamConfiguration.builder()
                                                                             .scalingPolicy(ScalingPolicy.fixed(1))
                                                                             .build())));
    }

    @Test(timeout = 10000)
    public void streamManagerImpltest() {
        ClientConfig config = ClientConfig.builder().build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(config, new SocketConnectionFactoryImpl(config));
        
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, cp);

        // Create and delete scope
        assertTrue(streamManager.createScope(SCOPE));
        assertTrue(streamManager.deleteScope(SCOPE));

        // Create scope twice
        assertTrue(streamManager.createScope(SCOPE));
        assertFalse(streamManager.createScope(SCOPE));
        assertTrue(streamManager.deleteScope(SCOPE));

        // Delete twice
        assertFalse(streamManager.deleteScope(SCOPE));
    }
}
