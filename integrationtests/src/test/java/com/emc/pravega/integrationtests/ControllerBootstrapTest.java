/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.testcommon.TestUtils;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
            zkTestServer = new TestingServer();
        } catch (Exception e) {
            Assert.fail("Failed starting ZK test server");
        }

        // 2. Start controller
        try {
            controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
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
                .scope(SCOPE)
                .streamName(STREAM)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        CompletableFuture<Boolean> streamStatus = controller.createStream(streamConfiguration);
        Assert.assertTrue(!streamStatus.isDone());

        // Create transaction should fail.
        CompletableFuture<UUID> txIdFuture = controller.createTransaction(new StreamImpl(SCOPE, STREAM),
                10000, 30000, 30000);

        try {
            txIdFuture.join();
            Assert.fail();
        } catch (CompletionException ce) {
            Assert.assertEquals(IllegalStateException.class, ce.getCause().getClass());
            Assert.assertTrue("Expected failure", true);
        }

        // Now start Pravega service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.builder().include(
                ServiceConfig.builder().with(ServiceConfig.LISTENING_PORT, servicePort).
                        with(ServiceConfig.CONTAINER_COUNT, 1)).build());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        // Ensure that create stream succeeds.
        try {
            Boolean status = streamStatus.join();
            Assert.assertEquals(true, status);
        } catch (CompletionException ce) {
            Assert.fail();
        }

        // Sleep for a while for initialize to complete
        boolean initialized = controllerWrapper.awaitTasksModuleInitialization(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(initialized);

        // Now create transaction should succeed.
        txIdFuture = controller.createTransaction(new StreamImpl(SCOPE, STREAM), 10000, 30000, 30000);

        try {
            UUID id = txIdFuture.join();
            Assert.assertNotNull(id);
        } catch (CompletionException ce) {
            Assert.fail();
        }

        controllerWrapper.awaitRunning();
    }
}
