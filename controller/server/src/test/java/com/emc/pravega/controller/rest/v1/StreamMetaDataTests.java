/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.rest.v1;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.emc.pravega.stream.ScalingPolicy.Type.FIXED_NUM_SEGMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for Stream metadata REST APIs.
 */
public class StreamMetaDataTests extends JerseyTest {

    ControllerService mockControllerService;
    StreamMetadataStore mockStreamStore;
    StreamMetadataResourceImpl streamMetadataResource;
    Future<Response> response;
    StreamProperty streamResponseActual;

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final String scope1 = "scope1";
    private final String resourceURI = "v1/scopes/" + scope1 + "/streams/" + stream1;
    private final String resourceURI2 = "v1/scopes/" + scope1 + "/streams/" + stream2;
    private final String streamResourceURI = "v1/scopes/" + scope1 + "/streams";
    private final com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy scalingPolicyCommon =
            new com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy();
    private final com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy retentionPolicyCommon =
            new com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy();
    private final com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy retentionPolicyCommon2 =
            new com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy();
    private final StreamProperty streamResponseExpected = new StreamProperty();
    private final StreamConfiguration streamConfiguration = new StreamConfigurationImpl(
            scope1, stream1, new ScalingPolicy(FIXED_NUM_SEGMENTS, 100L, 2, 2), new RetentionPolicy(123L));

    private final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest2 = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest3 = new UpdateStreamRequest();

    private final CompletableFuture<StreamConfiguration> streamConfigFuture = CompletableFuture.
            completedFuture(streamConfiguration);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus = CompletableFuture.
            completedFuture(CreateStreamStatus.SUCCESS);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus2 = CompletableFuture.
            completedFuture(CreateStreamStatus.STREAM_EXISTS);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus3 = CompletableFuture.
            completedFuture(CreateStreamStatus.FAILURE);
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus = CompletableFuture.
            completedFuture(UpdateStreamStatus.SUCCESS);
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus2 = CompletableFuture.
            completedFuture(UpdateStreamStatus.STREAM_NOT_FOUND);
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus3 = CompletableFuture.
            completedFuture(UpdateStreamStatus.FAILURE);

    @Before
    public void initialize() {
        scalingPolicyCommon.setType(
                com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingPolicyCommon.setTargetRate(100L);
        scalingPolicyCommon.setScaleFactor(2);
        scalingPolicyCommon.setMinNumSegments(2);
        retentionPolicyCommon.setRetentionTimeMillis(123L);
        retentionPolicyCommon2.setRetentionTimeMillis(null);
        streamResponseExpected.setScope(scope1);
        streamResponseExpected.setName(stream1);
        streamResponseExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseExpected.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest.setStreamName(stream1);
        createStreamRequest.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest2.setStreamName(stream1);
        createStreamRequest2.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest2.setRetentionPolicy(retentionPolicyCommon2);

        updateStreamRequest.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest.setRetentionPolicy(retentionPolicyCommon);
        updateStreamRequest2.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest2.setRetentionPolicy(retentionPolicyCommon);
        updateStreamRequest3.setScalingPolicy(scalingPolicyCommon);
        updateStreamRequest3.setRetentionPolicy(retentionPolicyCommon2);

        mockStreamStore = mock(StreamMetadataStore.class);
    }

    /**
     * Configure resource class.
     * @return JAX-RS application
     */
    @Override
    protected Application configure() {
        mockControllerService = mock(ControllerService.class);
        streamMetadataResource = new StreamMetadataResourceImpl(mockControllerService);

        return new ResourceConfig()
                .register(streamMetadataResource)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(mockControllerService).to(ControllerService.class);
                    }
                });
    }

    /**
     * Test for createStream REST API.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateStream() throws ExecutionException, InterruptedException {
        // Test to create a stream which doesn't exist
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest));
        assertEquals("Create Stream Status", 201, response.get().getStatus());
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to create a stream that already exists
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus2);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest));
        assertEquals("Create Stream Status", 409, response.get().getStatus());

        // Test for validation of create stream request object
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus3);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest2));
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Create Stream Status", 500, response.get().getStatus());
    }

    /**
     * Test for updateStreamConfig REST API
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        // Test to update an existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest));
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        assertEquals("Update Stream Status", 201, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to update an non-existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus2);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest2));
        assertEquals("Update Stream Status", 404, response.get().getStatus());

        // Test for validation of request object
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus3);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest3));
        assertEquals("Update Stream Status", 500, response.get().getStatus());
    }

    /**
     * Test for getStreamConfig REST API
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetStream() throws ExecutionException, InterruptedException {
        when(mockControllerService.getStreamStore()).thenReturn(mockStreamStore);

        // Test to get an existing stream
        when(mockStreamStore.getConfiguration(stream1)).thenReturn(streamConfigFuture);
        response = target(resourceURI).request().async().get();
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 200, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Get a non-existent stream
        when(mockStreamStore.getConfiguration(stream2)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new DataNotFoundException("Stream Not Found");
        }));
        response = target(resourceURI2).request().async().get();
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 404, response.get().getStatus());
    }

    private static void testExpectedVsActualObject(final StreamProperty expected, final StreamProperty actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals("StreamConfig: Scope Name ", expected.getScope(), actual.getScope());
        assertEquals("StreamConfig: Stream Name ",
                expected.getName(), actual.getName());
        assertEquals("StreamConfig: Scaling Policy: Type",
                expected.getScalingPolicy().getType(), actual.getScalingPolicy().getType());
        assertEquals("StreamConfig: Scaling Policy: Target Rate",
                expected.getScalingPolicy().getTargetRate(),
                actual.getScalingPolicy().getTargetRate());
        assertEquals("StreamConfig: Scaling Policy: Scale Factor",
                expected.getScalingPolicy().getScaleFactor(),
                actual.getScalingPolicy().getScaleFactor());
        assertEquals("StreamConfig: Scaling Policy: MinNumSegments",
                expected.getScalingPolicy().getMinNumSegments(),
                actual.getScalingPolicy().getMinNumSegments());
        assertEquals("StreamConfig: Retention Policy: MinNumSegments",
                expected.getRetentionPolicy().getRetentionTimeMillis(),
                actual.getRetentionPolicy().getRetentionTimeMillis());
    }
}
