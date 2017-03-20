/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.rest.v1;

import com.emc.pravega.controller.server.rest.CustomObjectMapperProvider;
import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.RetentionConfig;
import com.emc.pravega.controller.server.rest.generated.model.ScalingConfig;
import com.emc.pravega.controller.server.rest.generated.model.ScopesList;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.StreamState;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.StoreException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.emc.pravega.controller.server.rest.generated.model.RetentionConfig.TypeEnum;
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

    //Ensure each test completes within 5 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(5, TimeUnit.SECONDS);

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

    private final ScalingConfig scalingPolicyCommon = new ScalingConfig();
    private final RetentionConfig retentionPolicyCommon = new RetentionConfig();
    private final RetentionConfig retentionPolicyCommon2 = new RetentionConfig();
    private final StreamProperty streamResponseExpected = new StreamProperty();
    private final StreamConfiguration streamConfiguration = StreamConfiguration.builder()
            .scope(scope1)
            .streamName(stream1)
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(123L)))
            .build();

    private final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest3 = new CreateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest2 = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest3 = new UpdateStreamRequest();

    private final CompletableFuture<StreamConfiguration> streamConfigFuture = CompletableFuture.
            completedFuture(streamConfiguration);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.SUCCESS).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus2 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.STREAM_EXISTS).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus3 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.FAILURE).build());
    private final CompletableFuture<CreateStreamStatus> createStreamStatus4 = CompletableFuture.
            completedFuture(CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.SCOPE_NOT_FOUND).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SUCCESS).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus2 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus3 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build());
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus4 = CompletableFuture.
            completedFuture(UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build());

    @Before
    public void initialize() {
        scalingPolicyCommon.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingPolicyCommon.setTargetRate(100L);
        scalingPolicyCommon.setScaleFactor(2);
        scalingPolicyCommon.setMinSegments(2);
        retentionPolicyCommon.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyCommon.setValue(123L);
        retentionPolicyCommon2.setType(null);
        retentionPolicyCommon2.setValue(null);
        streamResponseExpected.setScopeName(scope1);
        streamResponseExpected.setStreamName(stream1);
        streamResponseExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseExpected.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest.setStreamName(stream1);
        createStreamRequest.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest.setRetentionPolicy(retentionPolicyCommon);

        createStreamRequest2.setStreamName(stream1);
        createStreamRequest2.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest2.setRetentionPolicy(retentionPolicyCommon2);

        createStreamRequest3.setStreamName(stream1);
        createStreamRequest3.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest3.setRetentionPolicy(retentionPolicyCommon);

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
     *
     * @return JAX-RS application
     */
    @Override
    protected Application configure() {
        mockControllerService = mock(ControllerService.class);
        streamMetadataResource = new StreamMetadataResourceImpl(mockControllerService);

        final ResourceConfig resourceConfig = new ResourceConfig().register(streamMetadataResource)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(mockControllerService).to(ControllerService.class);
                    }
                });
        resourceConfig.register(new CustomObjectMapperProvider());
        return resourceConfig;
    }

    /**
     * Test for createStream REST API.
     *
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

        // Test create stream for non-existent scope
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus4);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest3));
        assertEquals("Create Stream Status for non-existent scope", 404, response.get().getStatus());
    }

    /**
     * Test for updateStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        // Test to update an existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest));
        assertEquals("Update Stream Status", 200, response.get().getStatus());
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test sending extra fields in the request object to check if json parser can handle it.
        response = target(resourceURI).request().async().put(Entity.json(createStreamRequest));
        assertEquals("Update Stream Status", 200, response.get().getStatus());
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to update an non-existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus2);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest2));
        assertEquals("Update Stream Status", 404, response.get().getStatus());

        // Test for validation of request object
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus3);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest3));
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Update Stream Status", 500, response.get().getStatus());

        // Test to update stream for non-existent scope
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus4);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest));
        assertEquals("Update Stream Status", 404, response.get().getStatus());
    }

    /**
     * Test for getStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetStream() throws ExecutionException, InterruptedException {
        when(mockControllerService.getStream(scope1, stream1)).thenReturn(streamConfigFuture);

        // Test to get an existing stream
        response = target(resourceURI).request().async().get();
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 200, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Get a non-existent stream
        when(mockControllerService.getStream(scope1, stream2)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new DataNotFoundException("Stream Not Found");
        }));
        response = target(resourceURI2).request().async().get();
        streamResponseActual = response.get().readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 404, response.get().getStatus());
    }

    /**
     * Test for deleteStream REST API
     *
     * @throws Exception
     */
    @Test
    public void testDeleteStream() throws Exception {
        final String resourceURI = "v1/scopes/scope1/streams/stream1";

        // Test to delete a sealed stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.SUCCESS).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Stream response code", 204, response.get().getStatus());

        // Test to delete a unsealed stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Stream response code", 412, response.get().getStatus());

        // Test to delete a non existent stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Stream response code", 404, response.get().getStatus());

        // Test to delete a stream giving an internal server error
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Stream response code", 500, response.get().getStatus());
    }

    /**
     * Test for createScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateScope() throws ExecutionException, InterruptedException {
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest().scopeName(scope1);
        final String resourceURI = "v1/scopes/";

        // Test to create a new scope.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build()));
        response = target(resourceURI).request().async().post(Entity.json(createScopeRequest));
        assertEquals("Create Scope response code", 201, response.get().getStatus());

        // Test to create an existing scope.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        response = target(resourceURI).request().async().post(Entity.json(createScopeRequest));
        assertEquals("Create Scope response code", 409, response.get().getStatus());

        // create scope failure.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().async().post(Entity.json(createScopeRequest));
        assertEquals("Create Scope response code", 500, response.get().getStatus());
    }

    /**
     * Test for deleteScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        final String resourceURI = "v1/scopes/scope1";

        // Test to delete a scope.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Scope response code", 204, response.get().getStatus());

        // Test to delete scope with existing streams.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Scope response code", 412, response.get().getStatus());

        // Test to delete non-existing scope.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Scope response code", 404, response.get().getStatus());

        // Test delete scope failure.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().async().delete();
        assertEquals("Delete Scope response code", 500, response.get().getStatus());
    }

    /**
     * Test to retrieve a scope.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetScope() throws ExecutionException, InterruptedException {
        final String resourceURI = "v1/scopes/scope1";
        final String resourceURI2 = "v1/scopes/scope2";

        // Test to get existent scope
        when(mockControllerService.getScope(scope1)).thenReturn(CompletableFuture.completedFuture("scope1"));
        response = target(resourceURI).request().async().get();
        assertEquals("Get existent scope", 200, response.get().getStatus());

        // Test to get non-existent scope
        when(mockControllerService.getScope("scope2")).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new StoreException.NodeNotFoundException();
        }));
        response = target(resourceURI2).request().async().get();
        assertEquals("Get non existent scope", 404, response.get().getStatus());

        //Test for get scope failure.
        final CompletableFuture<String> completableFuture2 = new CompletableFuture<>();
        completableFuture2.completeExceptionally(new Exception());
        when(mockControllerService.getScope(scope1)).thenReturn(completableFuture2);
        response = target(resourceURI).request().async().get();
        assertEquals("Get scope fail test", 500, response.get().getStatus());
    }

    /**
     * Test for listScopes REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListScopes() throws ExecutionException, InterruptedException {
        final String resourceURI = "v1/scopes";

        // Test to list scopes.
        List<String> scopesList = Arrays.asList("scope1", "scope2");
        when(mockControllerService.listScopes()).thenReturn(CompletableFuture.completedFuture(scopesList));
        response = target(resourceURI).request().async().get();
        final ScopesList scopesList1 = response.get().readEntity(ScopesList.class);
        assertEquals("List Scopes response code", 200, response.get().getStatus());
        assertEquals("List count", scopesList1.getScopes().size(), 2);
        assertEquals("List element", scopesList1.getScopes().get(0).getScopeName(), "scope1");
        assertEquals("List element", scopesList1.getScopes().get(1).getScopeName(), "scope2");

        // Test for list scopes failure.
        final CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listScopes()).thenReturn(completableFuture);
        response = target(resourceURI).request().async().get();
        assertEquals("List Scopes response code", 500, response.get().getStatus());
    }

    /**
     * Test for listStreams REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListStreams() throws ExecutionException, InterruptedException {
        final String resourceURI = "v1/scopes/scope1/streams";

        final StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTimeMillis(123L))
                .build();

        final StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(stream2)
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTimeMillis(123L))
                .build();

        // Test to list streams.
        List<StreamConfiguration> streamsList = Arrays.asList(streamConfiguration1, streamConfiguration2);

        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(CompletableFuture.completedFuture(streamsList));
        response = target(resourceURI).request().async().get();
        final StreamsList streamsList1 = response.get().readEntity(StreamsList.class);
        assertEquals("List Streams response code", 200, response.get().getStatus());
        assertEquals("List count", streamsList1.getStreams().size(), 2);
        assertEquals("List element", streamsList1.getStreams().get(0).getStreamName(), "stream1");
        assertEquals("List element", streamsList1.getStreams().get(1).getStreamName(), "stream2");

        // Test for list streams for invalid scope.
        final CompletableFuture<List<StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(new DataNotFoundException(""));
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture1);
        response = target(resourceURI).request().async().get();
        assertEquals("List Streams response code", 404, response.get().getStatus());

        // Test for list streams failure.
        final CompletableFuture<List<StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture);
        response = target(resourceURI).request().async().get();
        assertEquals("List Streams response code", 500, response.get().getStatus());
    }

    /**
     * Test for updateStreamState REST API.
     */
    @Test
    public void testUpdateStreamState() throws Exception {
        final String resourceURI = "v1/scopes/scope1/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("scope1", "stream1")).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().async().put(Entity.json(streamState));
        assertEquals("Update Stream State response code", 200, response.get().getStatus());

        // Test to seal a non existent scope.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().async().put(Entity.json(streamState));
        assertEquals("Update Stream State response code", 404, response.get().getStatus());

        // Test to seal a non existent stream.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().async().put(Entity.json(streamState));
        assertEquals("Update Stream State response code", 404, response.get().getStatus());

        // Test to check failure.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().async().put(Entity.json(streamState));
        assertEquals("Update Stream State response code", 500, response.get().getStatus());
    }

    private static void testExpectedVsActualObject(final StreamProperty expected, final StreamProperty actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals("StreamConfig: Scope Name ", expected.getScopeName(), actual.getScopeName());
        assertEquals("StreamConfig: Stream Name ",
                expected.getStreamName(), actual.getStreamName());
        assertEquals("StreamConfig: Scaling Policy: Type",
                expected.getScalingPolicy().getType(), actual.getScalingPolicy().getType());
        assertEquals("StreamConfig: Scaling Policy: Target Rate",
                expected.getScalingPolicy().getTargetRate(),
                actual.getScalingPolicy().getTargetRate());
        assertEquals("StreamConfig: Scaling Policy: Scale Factor",
                expected.getScalingPolicy().getScaleFactor(),
                actual.getScalingPolicy().getScaleFactor());
        assertEquals("StreamConfig: Scaling Policy: MinNumSegments",
                expected.getScalingPolicy().getMinSegments(),
                actual.getScalingPolicy().getMinSegments());
        assertEquals("StreamConfig: Retention Policy: type",
                expected.getRetentionPolicy().getType(),
                actual.getRetentionPolicy().getType());
        assertEquals("StreamConfig: Retention Policy: value",
                expected.getRetentionPolicy().getValue(),
                actual.getRetentionPolicy().getValue());
    }
}

