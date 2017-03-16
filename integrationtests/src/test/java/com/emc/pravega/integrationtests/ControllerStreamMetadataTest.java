/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import com.emc.pravega.testcommon.TestUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.test.TestingServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void setUp() {
        final int controllerPort = TestUtils.randomPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.randomPort();
        final int containerCount = 4;

        try {
            // 1. Start ZK
            this.zkTestServer = new TestingServer();

            // 2. Start Pravega service.
            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

            this.server = new PravegaConnectionListener(false, servicePort, store);
            this.server.startListening();

            // 3. Start controller
            this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                    controllerPort, serviceHost, servicePort, containerCount);
            this.controller = controllerWrapper.getController();
            this.streamConfiguration = StreamConfiguration.builder()
                    .scope(SCOPE)
                    .streamName(STREAM)
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
        } catch (Exception e) {
            log.error("Error during setup", e);
            Assert.fail();
        }
    }

    @After
    public void tearDown() {
        try {
            if (this.zkTestServer != null) {
                this.zkTestServer.close();
                this.zkTestServer = null;
            }
            if (this.server != null) {
                this.server.close();
                this.server = null;
            }
            if (this.controllerWrapper != null) {
                this.controllerWrapper.close();
                this.controllerWrapper = null;
            }
        } catch (Exception e) {
            log.warn("Exception while tearing down", e);
        }
    }

    @Test(timeout = 2000000)
    public void streamMetadataTest() throws Exception {
        // Create test scope. This operation should succeed.
        CreateScopeStatus scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // Delete the test scope. This operation should also succeed.
        DeleteScopeStatus deleteScopeStatus = controller.deleteScope(SCOPE).join();
        Assert.assertEquals(DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.getStatus());

        // Try creating a stream. It should fail, since the scope does not exist.
        CreateStreamStatus streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.SCOPE_NOT_FOUND, streamStatus.getStatus());

        // Again create the scope.
        scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // Try creating the stream again. It should succeed now, since the scope exists.
        streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        // Delete test scope. This operation should fail, since it is not empty.
        deleteScopeStatus = controller.deleteScope(SCOPE).join();
        Assert.assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, deleteScopeStatus.getStatus());

        // Try creating already existing scope. It should fail
        scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(CreateScopeStatus.Status.SCOPE_EXISTS, scopeStatus.getStatus());

        // Try creating already existing stream. It should fail.
        streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, streamStatus.getStatus());

        // Delete test stream. This operation should fail, since it is not yet SEALED.
        DeleteStreamStatus deleteStreamStatus = controller.deleteStream(SCOPE, STREAM).join();
        Assert.assertEquals(DeleteStreamStatus.Status.STREAM_NOT_SEALED, deleteStreamStatus.getStatus());

        // Seal the test stream. This operation should succeed.
        UpdateStreamStatus updateStreamStatus = controller.sealStream(SCOPE, STREAM).join();
        Assert.assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStreamStatus.getStatus());

        // Delete test stream. This operation should succeed.
        deleteStreamStatus = controller.deleteStream(SCOPE, STREAM).join();
        Assert.assertEquals(DeleteStreamStatus.Status.SUCCESS, deleteStreamStatus.getStatus());

        // Delete test stream again. Now it should fail.
        deleteStreamStatus = controller.deleteStream(SCOPE, STREAM).join();
        Assert.assertEquals(DeleteStreamStatus.Status.STREAM_NOT_FOUND, deleteStreamStatus.getStatus());

        // Delete test scope. This operation sholud succeed.
        deleteScopeStatus = controller.deleteScope(SCOPE).join();
        Assert.assertEquals(DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.getStatus());

        // Delete a non-existent scope. This operation should fail.
        deleteScopeStatus = controller.deleteScope("non_existent_scope").join();
        Assert.assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteScopeStatus.getStatus());

        // Create a scope with invalid characters. It should fail.
        scopeStatus = controller.createScope("abc/def").join();
        Assert.assertEquals(CreateScopeStatus.Status.INVALID_SCOPE_NAME, scopeStatus.getStatus());

        // Try creating stream with invalid characters. It should fail.
        streamStatus = controller.createStream(StreamConfiguration.builder()
                .scope(SCOPE).streamName("abc/def").scalingPolicy(ScalingPolicy.fixed(1)).build()).join();
        Assert.assertEquals(CreateStreamStatus.Status.INVALID_STREAM_NAME, streamStatus.getStatus());
    }


    @Test(timeout = 10000)
    public void streamManagerImpltest() {
        StreamManager streamManager = new StreamManagerImpl(SCOPE, controller);

        streamManager.createScope(SCOPE);
        streamManager.deleteScope(SCOPE);
    }
}
