/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.controller.pravega.server;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.testcommon.TestUtils;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ControllerServiceTest {

    private final int controllerPort = TestUtils.randomPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.randomPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    
    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServer();
        
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        
        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();
        
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                                                                    controllerPort, serviceHost, servicePort, containerCount);
    }
    
    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }
    
    
    @Test(timeout = 40000)
    public void streamMetadataTest() throws Exception {
        final String scope = "testScope";
        final String stream = "testStream";

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scope(scope)
                .streamName(stream)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        // Create test scope. This operation should succeed.
        CreateScopeStatus scopeStatus = controller.createScope(scope).join();
        Assert.assertEquals(CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // Delete the test scope. This operation should also succeed.
        DeleteScopeStatus deleteScopeStatus = controller.deleteScope(scope).join();
        Assert.assertEquals(DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.getStatus());

        // Try creating a stream. It should fail, since the scope does not exist.
        CreateStreamStatus streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.SCOPE_NOT_FOUND, streamStatus.getStatus());

        // Again create the scope.
        scopeStatus = controller.createScope(scope).join();
        Assert.assertEquals(CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // Try creating the stream again. It should succeed now, since the scope exists.
        streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        // Delete test scope. This operation should fail, since it is not empty.
        deleteScopeStatus = controller.deleteScope(scope).join();
        Assert.assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, deleteScopeStatus.getStatus());

        // Delete a non-existent scope. This operation should fail.
        deleteScopeStatus = controller.deleteScope("non_existent_scope").join();
        Assert.assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteScopeStatus.getStatus());

        // Create a scope with invalid characters. It should fail.
        scopeStatus = controller.createScope("abc/def").join();
        Assert.assertEquals(CreateScopeStatus.Status.INVALID_SCOPE_NAME, scopeStatus.getStatus());

        // Try creating already existing scope. It should fail
        scopeStatus = controller.createScope(scope).join();
        Assert.assertEquals(CreateScopeStatus.Status.SCOPE_EXISTS, scopeStatus.getStatus());

        // Try creating stream with invalid characters. It should fail.
        streamStatus = controller.createStream(StreamConfiguration.builder()
                .scope(scope).streamName("abc/def").scalingPolicy(ScalingPolicy.fixed(1)).build()).join();
        Assert.assertEquals(CreateStreamStatus.Status.INVALID_STREAM_NAME, streamStatus.getStatus());

        // Try creating already existing stream. It should fail.
        streamStatus = controller.createStream(streamConfiguration).join();
        Assert.assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, streamStatus.getStatus());
    }
    
    
    @Test(timeout = 40000)
    public void testControllerService() throws Exception {
        final String scope1 = "scope1";
        final String scope2 = "scope2";
        controllerWrapper.getControllerService().createScope("scope1").get();
        controllerWrapper.getControllerService().createScope("scope2").get();
        Controller controller = controllerWrapper.getController();

        final String streamName1 = "stream1";
        final String streamName2 = "stream2";
        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
        final StreamConfiguration config1 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(scalingPolicy)
                .build();
        final StreamConfiguration config2 = StreamConfiguration.builder()
                .scope(scope2)
                .streamName(streamName1)
                .scalingPolicy(scalingPolicy)
                .build();
        final StreamConfiguration config3 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName2)
                .scalingPolicy(new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 3))
                .build();

        createAStream(controller, config1);
        //Same name in different scope
        createAStream(controller, config2);
        //Different name in same scope
        createAStream(controller, config3);
        
        final String scopeSeal = "scopeSeal";
        final String streamNameSeal = "streamSeal";
        sealAStream(controllerWrapper, controller, scalingPolicy, scopeSeal, streamNameSeal);
        
        sealASealedStream(controller, scopeSeal, streamNameSeal);
 
        sealNonExistantStream(controller, scopeSeal);

        streamDuplicationNotAllowed(controller, config1);
       
        //update stream config section

        updateStreamName(controller, scope1, scalingPolicy);

        updateScalingPolicy(controller, scope1, streamName1);

        updateTargetRate(controller, scope1, streamName1);

        updateScaleFactor(controller, scope1, streamName1);

        updataMinSegmentes(controller, scope1, streamName1);

        alterConfigOfNonExistantStream(controller);

        //get currently active segments

        getActiveSegments(controller, scope1, streamName1);

        getActiveSegmentsForNonExistentStream(controller);

        //get positions at a given time stamp

        getSegmentsAtTime(controller, scope1, streamName1);
        getSegmentsAtTime(controller, scope1, streamName2);

        getSegmentsForNonExistentStream(controller);
        
        getSegmentsBeforeCreation(controller, scope1, streamName1);

        getSegmentsAfterCreation(controller, scope1, streamName1);
    }

    private static void getSegmentsAfterCreation(Controller controller, final String scope,
                                                 final String streamName) throws InterruptedException,
                                                                           ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis() + 3600);
        assertFalse("FAILURE: Fetching positions at given time in furture after stream creation failed", segments.get().isEmpty());
    }

    private static void getSegmentsBeforeCreation(Controller controller, final String scope,
                                                  final String streamName) throws InterruptedException,
                                                                            ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis() - 36000);
        assertFalse("FAILURE: Fetching positions at given time before stream creation failed", segments.get().size() != controller.getCurrentSegments(scope, streamName).get().getSegments().size());
       
    }

    private static void getSegmentsForNonExistentStream(Controller controller) throws InterruptedException {
        Stream stream = new StreamImpl("scope", "streamName");
        try {
            CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(stream, System.currentTimeMillis());
            assertTrue("FAILURE: Fetching positions for non existent stream", segments.get().isEmpty());
            
            System.err.println("SUCCESS: Positions cannot be fetched for non existent stream");
        } catch (ExecutionException | CompletionException e) {
            assertTrue("FAILURE: Fetching positions for non existent stream", e.getCause() instanceof DataNotFoundException);
            System.err.println("SUCCESS: Positions cannot be fetched for non existent stream");
        }
    }

    private static void getSegmentsAtTime(Controller controller, final String scope,
                                            final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis());
        assertFalse("FAILURE: Fetching positions at given time stamp failed", segments.get().isEmpty()); 
    }

    private static void getActiveSegmentsForNonExistentStream(Controller controller) throws InterruptedException {
        try {
            CompletableFuture<StreamSegments> getActiveSegments = controller.getCurrentSegments("scope", "streamName");
            assertTrue("FAILURE: Fetching active segments for non existent stream", getActiveSegments.get().getSegments().isEmpty());
        } catch (ExecutionException | CompletionException e) {
            assertTrue("FAILURE: Fetching active segments for non existent stream", e.getCause() instanceof DataNotFoundException);
        }
    }

    private static void getActiveSegments(Controller controller, final String scope,
                                          final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<StreamSegments> getActiveSegments = controller.getCurrentSegments(scope, streamName);
        assertFalse("FAILURE: Fetching active segments failed", getActiveSegments.get().getSegments().isEmpty());
        
    }

    private static void alterConfigOfNonExistantStream(Controller controller) throws InterruptedException,
                                                                              ExecutionException {
        final StreamConfiguration config = StreamConfiguration.builder()
                .scope("scope")
                .streamName("streamName")
                .scalingPolicy(new ScalingPolicy(
                        ScalingPolicy.Type.FIXED_NUM_SEGMENTS,
                        200, 2, 3))
                .build();
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(config);
        assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStatus.get().getStatus());
    }

    private static void updataMinSegmentes(Controller controller, final String scope,
                                           final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(StreamConfiguration.builder()
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(new ScalingPolicy(
                        ScalingPolicy.Type.FIXED_NUM_SEGMENTS,
                        100, 2, 3))
                .build());
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStatus.get().getStatus());
    }

    private static void updateScaleFactor(Controller controller, final String scope,
                                          final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(StreamConfiguration.builder()
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS,
                        100, 3, 2))
                .build());
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStatus.get().getStatus());
    }

    private static void updateTargetRate(Controller controller, final String scope,
                                         final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(StreamConfiguration.builder()
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS,
                        200, 2, 2))
                .build());
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStatus.get().getStatus());
    }

    private static void updateScalingPolicy(Controller controller, final String scope,
                                            final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(StreamConfiguration.builder()
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_KBYTES_PER_SEC, 100, 2, 2))
                .build());
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStatus.get().getStatus());
    }

    private static void updateStreamName(Controller controller, final String scope,
                                         final ScalingPolicy scalingPolicy) throws InterruptedException,
                                                                            ExecutionException {
        CompletableFuture<UpdateStreamStatus> updateStatus;
        updateStatus = controller.alterStream(StreamConfiguration.builder()
                .scope(scope)
                .streamName("stream4")
                .scalingPolicy(scalingPolicy)
                .build());
        assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStatus.get().getStatus());
    }

    private static void sealAStream(ControllerWrapper controllerWrapper, Controller controller,
                                   final ScalingPolicy scalingPolicy, final String scopeSeal,
                                   final String streamNameSeal) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope("scopeSeal").get();

        final StreamConfiguration configSeal = StreamConfiguration.builder()
                .scope(scopeSeal)
                .streamName(streamNameSeal)
                .scalingPolicy(scalingPolicy)
                .build();
        CreateStreamStatus createStream3Status = controller.createStream(configSeal).get();
        assertEquals(CreateStreamStatus.Status.SUCCESS,  createStream3Status.getStatus());

        @SuppressWarnings("unused")
        StreamSegments result = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        UpdateStreamStatus sealStatus = controller.sealStream(scopeSeal, streamNameSeal).get();
        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealStatus.getStatus());

        StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        assertTrue("FAILURE: No active segments should be present in a sealed stream", currentSegs.getSegments().isEmpty());
        
    }

    private static void createAStream(Controller controller, final StreamConfiguration config) throws InterruptedException,
                                                                         ExecutionException {
        CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);
        assertEquals(CreateStreamStatus.Status.SUCCESS, createStatus.get().getStatus());
    }

    private static void sealNonExistantStream(Controller controller,
                                              final String scopeSeal) throws InterruptedException, ExecutionException {
        UpdateStreamStatus errSealStatus = controller.sealStream(scopeSeal, "nonExistentStream").get();
        assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, errSealStatus.getStatus());
    }

    private static void streamDuplicationNotAllowed(Controller controller, final StreamConfiguration config) throws InterruptedException,
                                                                                       ExecutionException {
        CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);
        assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, createStatus.get().getStatus());
    }

    private static void sealASealedStream(Controller controller, final String scopeSeal,
                                          final String streamNameSeal) throws InterruptedException, ExecutionException {
        UpdateStreamStatus reSealStatus = controller.sealStream(scopeSeal, streamNameSeal).get();
        assertEquals(UpdateStreamStatus.Status.SUCCESS, reSealStatus.getStatus());

        StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        assertTrue("FAILURE: No active segments should be present in a sealed stream", currentSegs.getSegments().isEmpty());
        
    }

}
