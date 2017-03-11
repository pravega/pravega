/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.testcommon.TestUtils;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Controller stream metadata tests.
 */
public class ControllerStreamMetadataTest {
    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";

    @Test(timeout = 2000000)
    public void streamMetadataTest() throws Exception {
        final int controllerPort = TestUtils.randomPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.randomPort();
        final int containerCount = 4;

        // 1. Start ZK
        @Cleanup
        TestingServer zkTestServer = new TestingServer();

        // 2. Start Pravega service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        // 3. Start controller
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                controllerPort, serviceHost, servicePort, containerCount);
        Controller controller = controllerWrapper.getController();

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(STREAM)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

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

        // Delete a non-existent scope. This operation should fail.
        deleteScopeStatus = controller.deleteScope("non_existent_scope").join();
        Assert.assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteScopeStatus.getStatus());

        // Create a scope with invalid characters. It should fail.
        scopeStatus = controller.createScope("abc/def").join();
        Assert.assertEquals(CreateScopeStatus.Status.INVALID_SCOPE_NAME, scopeStatus.getStatus());

        // Try creating already existing scope. It should fail
        scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(CreateScopeStatus.Status.SCOPE_EXISTS, scopeStatus.getStatus());

        // Try creating stream with invalid characters. It should fail.
        streamStatus = controller.createStream(StreamConfiguration.builder()
                .scope(SCOPE).streamName("abc/def").scalingPolicy(ScalingPolicy.fixed(1)).build()).join();
        Assert.assertEquals(CreateStreamStatus.Status.INVALID_STREAM_NAME, streamStatus.getStatus());

        // Try creating already existing stream. It should fail.
        streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, streamStatus.getStatus());
    }
}
