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

import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.test.common.TestUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
public class ControllerBootstrapTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();
    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;

    @Before
    public void setup() {
        final String serviceHost = "localhost";
        final int containerCount = 4;

        // 1. Start ZK
        try {
            zkTestServer = new TestingServerStarter().start();
        } catch (Exception e) {
            Assert.fail("Failed starting ZK test server");
        }

        // 2. Start controller
        try {
            controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                    controllerPort, serviceHost, servicePort, containerCount);
        } catch (Exception e) {
            Assert.fail("Failed starting ControllerWrapper");
        }
    }

    @After
    public void cleanup() throws Exception {
        if (controllerWrapper != null) {
            controllerWrapper.close();
        }
        if (server != null) {
            server.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test(timeout = 20000)
    public void bootstrapTest() throws Exception {
        Controller controller = controllerWrapper.getController();

        // Create test scope. This operation should succeed.
        Boolean scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(true, scopeStatus);

        // Try creating a stream. It should not complete until Pravega host has started.
        // After Pravega host starts, stream should be successfully created.
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        CompletableFuture<Boolean> streamStatus = controller.createStream(SCOPE, STREAM, streamConfiguration);
        Assert.assertTrue(!streamStatus.isDone());
        
        // Now start Pravega service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store, mock(TableStore.class));
        server.startListening();

        // Ensure that create stream succeeds.
        try {
            Boolean status = streamStatus.join();
            Assert.assertEquals(true, status);
        } catch (CompletionException ce) {
            Assert.fail();
        }
        
        // Now create transaction should succeed.
        CompletableFuture<TxnSegments> txIdFuture = controller.createTransaction(new StreamImpl(SCOPE, STREAM), 10000);

        try {
            TxnSegments id = txIdFuture.join();
            Assert.assertNotNull(id);
        } catch (CompletionException ce) {
            Assert.fail();
        }

        controllerWrapper.awaitRunning();
    }
}
