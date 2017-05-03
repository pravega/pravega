/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.rest.v1;

import io.pravega.shared.NameUtils;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rest.CustomObjectMapperProvider;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import io.pravega.controller.store.stream.DataNotFoundException;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import io.pravega.test.common.TestUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.controller.server.rest.generated.model.RetentionConfig.TypeEnum;
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

    //Ensure each test completes within 20 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(20, TimeUnit.SECONDS);

    ControllerService mockControllerService;
    StreamMetadataStore mockStreamStore;
    StreamMetadataResourceImpl streamMetadataResource;
    StreamProperty streamResponseActual;

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final String stream3 = "stream3";
    private final String stream4 = "stream4";
    private final String scope1 = "scope1";
    private final String resourceURI = "v1/scopes/" + scope1 + "/streams/" + stream1;
    private final String resourceURI2 = "v1/scopes/" + scope1 + "/streams/" + stream2;
    private final String streamResourceURI = "v1/scopes/" + scope1 + "/streams";

    private final ScalingConfig scalingPolicyCommon = new ScalingConfig();
    private final ScalingConfig scalingPolicyCommon2 = new ScalingConfig();
    private final RetentionConfig retentionPolicyCommon = new RetentionConfig();
    private final RetentionConfig retentionPolicyCommon2 = new RetentionConfig();
    private final RetentionConfig retentionPolicyCommon3 = new RetentionConfig();
    private final StreamProperty streamResponseExpected = new StreamProperty();
    private final StreamProperty streamResponseExpected2 = new StreamProperty();
    private final StreamProperty streamResponseExpected3 = new StreamProperty();
    private final StreamConfiguration streamConfiguration = StreamConfiguration.builder()
            .scope(scope1)
            .streamName(stream1)
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(123L)))
            .build();

    private final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest3 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest4 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest5 = new CreateStreamRequest();
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
        scalingPolicyCommon.setTargetRate(100);
        scalingPolicyCommon.setScaleFactor(2);
        scalingPolicyCommon.setMinSegments(2);

        scalingPolicyCommon2.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingPolicyCommon2.setMinSegments(2);

        retentionPolicyCommon.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyCommon.setValue(123L);

        retentionPolicyCommon2.setType(null);
        retentionPolicyCommon2.setValue(null);

        retentionPolicyCommon3.setType(TypeEnum.INFINITE);

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

        createStreamRequest4.setStreamName(stream3);
        createStreamRequest4.setScalingPolicy(scalingPolicyCommon);
        createStreamRequest4.setRetentionPolicy(retentionPolicyCommon3);

        // stream 4 where targetRate and scalingFactor for Scaling Policy are null
        createStreamRequest5.setStreamName(stream4);
        createStreamRequest5.setScalingPolicy(scalingPolicyCommon2);
        createStreamRequest5.setRetentionPolicy(retentionPolicyCommon);

        streamResponseExpected2.setScopeName(scope1);
        streamResponseExpected2.setStreamName(stream3);
        streamResponseExpected2.setScalingPolicy(scalingPolicyCommon);
        streamResponseExpected2.setRetentionPolicy(retentionPolicyCommon3);

        streamResponseExpected3.setScopeName(scope1);
        streamResponseExpected3.setStreamName(stream4);
        streamResponseExpected3.setScalingPolicy(scalingPolicyCommon2);
        streamResponseExpected3.setRetentionPolicy(retentionPolicyCommon);

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
        this.forceSet(TestProperties.CONTAINER_PORT, String.valueOf(TestUtils.getAvailableListenPort()));

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
        Response response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to create a stream which doesn't exist and have Retention Policy INFINITE
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus);
        response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest4)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected2, streamResponseActual);

        // Test to create a stream with internal stream name
        final CreateStreamRequest streamRequest = new CreateStreamRequest();
        streamRequest.setStreamName(NameUtils.getInternalNameForStream("stream"));
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus2);
        response = target(streamResourceURI).request().buildPost(Entity.json(streamRequest)).invoke();
        assertEquals("Create Stream Status", 400, response.getStatus());

        // Test to create a stream which doesn't exist and have Scaling Policy FIXED_NUM_SEGMENTS
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus);
        response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest5)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected3, streamResponseActual);

        // Test to create a stream that already exists
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus2);
        response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 409, response.getStatus());

        // Test for validation of create stream request object
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus3);
        response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest2)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Create Stream Status", 500, response.getStatus());

        // Test create stream for non-existent scope
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus4);
        response = target(streamResourceURI).request().buildPost(Entity.json(createStreamRequest3)).invoke();
        assertEquals("Create Stream Status for non-existent scope", 404, response.getStatus());
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
        Response response = target(resourceURI).request().buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test sending extra fields in the request object to check if json parser can handle it.
        response = target(resourceURI).request().buildPut(Entity.json(createStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to update an non-existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus2);
        response = target(resourceURI).request().buildPut(Entity.json(updateStreamRequest2)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());

        // Test for validation of request object
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus3);
        response = target(resourceURI).request().buildPut(Entity.json(updateStreamRequest3)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Update Stream Status", 500, response.getStatus());

        // Test to update stream for non-existent scope
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus4);
        response = target(resourceURI).request().buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());
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
        Response response = target(resourceURI).request().buildGet().invoke();
        streamResponseActual = response.readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Get a non-existent stream
        when(mockControllerService.getStream(scope1, stream2)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new DataNotFoundException("Stream Not Found");
        }));
        response = target(resourceURI2).request().buildGet().invoke();
        streamResponseActual = response.readEntity(StreamProperty.class);
        assertEquals("Get Stream Config Status", 404, response.getStatus());
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
        Response response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Stream response code", 204, response.getStatus());

        // Test to delete a unsealed stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Stream response code", 412, response.getStatus());

        // Test to delete a non existent stream
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Stream response code", 404, response.getStatus());

        // Test to delete a stream giving an internal server error
        when(mockControllerService.deleteStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Stream response code", 500, response.getStatus());
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
        Response response = target(resourceURI).request().buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 201, response.getStatus());

        // Test to create an existing scope.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        response = target(resourceURI).request().buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 409, response.getStatus());

        // create scope failure.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 500, response.getStatus());

        // Test to create an invalid scope name.
        when(mockControllerService.createScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        createScopeRequest.setScopeName("_system");
        response = target(resourceURI).request().buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 400, response.getStatus());
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
        Response response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Scope response code", 204, response.getStatus());

        // Test to delete scope with existing streams.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Scope response code", 412, response.getStatus());

        // Test to delete non-existing scope.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Scope response code", 404, response.getStatus());

        // Test delete scope failure.
        when(mockControllerService.deleteScope(scope1)).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build()));
        response = target(resourceURI).request().buildDelete().invoke();
        assertEquals("Delete Scope response code", 500, response.getStatus());
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
        Response response = target(resourceURI).request().buildGet().invoke();
        assertEquals("Get existent scope", 200, response.getStatus());

        // Test to get non-existent scope
        when(mockControllerService.getScope("scope2")).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new StoreException.NodeNotFoundException();
        }));
        response = target(resourceURI2).request().buildGet().invoke();
        assertEquals("Get non existent scope", 404, response.getStatus());

        //Test for get scope failure.
        final CompletableFuture<String> completableFuture2 = new CompletableFuture<>();
        completableFuture2.completeExceptionally(new Exception());
        when(mockControllerService.getScope(scope1)).thenReturn(completableFuture2);
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("Get scope fail test", 500, response.getStatus());
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
        Response response = target(resourceURI).request().buildGet().invoke();
        final ScopesList scopesList1 = response.readEntity(ScopesList.class);
        assertEquals("List Scopes response code", 200, response.getStatus());
        assertEquals("List count", scopesList1.getScopes().size(), 2);
        assertEquals("List element", scopesList1.getScopes().get(0).getScopeName(), "scope1");
        assertEquals("List element", scopesList1.getScopes().get(1).getScopeName(), "scope2");

        // Test for list scopes failure.
        final CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listScopes()).thenReturn(completableFuture);
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Scopes response code", 500, response.getStatus());
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
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        final StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(stream2)
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        // Test to list streams.
        List<StreamConfiguration> streamsList = Arrays.asList(streamConfiguration1, streamConfiguration2);

        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(CompletableFuture.completedFuture(streamsList));
        Response response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        final StreamsList streamsList1 = response.readEntity(StreamsList.class);
        assertEquals("List count", streamsList1.getStreams().size(), 2);
        assertEquals("List element", streamsList1.getStreams().get(0).getStreamName(), "stream1");
        assertEquals("List element", streamsList1.getStreams().get(1).getStreamName(), "stream2");

        // Test for list streams for invalid scope.
        final CompletableFuture<List<StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(new DataNotFoundException(""));
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture1);
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Streams response code", 404, response.getStatus());

        // Test for list streams failure.
        final CompletableFuture<List<StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(completableFuture);
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Streams response code", 500, response.getStatus());

        // Test for filtering streams.
        final StreamConfiguration streamConfiguration3 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(NameUtils.getInternalNameForStream("stream3"))
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.INFINITE)
                .build();
        List<StreamConfiguration> allStreamsList = Arrays.asList(streamConfiguration1, streamConfiguration2,
                streamConfiguration3);
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(
                CompletableFuture.completedFuture(allStreamsList));
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        StreamsList streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 2, streamsListResp.getStreams().size());
        assertEquals("List element", "stream1", streamsListResp.getStreams().get(0).getStreamName());
        assertEquals("List element", "stream2", streamsListResp.getStreams().get(1).getStreamName());
        response = target(resourceURI).queryParam("showInternalStreams", "true").request().buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 1, streamsListResp.getStreams().size());
        assertEquals("List element", NameUtils.getInternalNameForStream("stream3"),
                streamsListResp.getStreams().get(0).getStreamName());

        // Test to list large number of streams.
        streamsList = Collections.nCopies(1000, streamConfiguration1);
        when(mockControllerService.listStreamsInScope("scope1")).thenReturn(CompletableFuture.completedFuture(streamsList));
        response = target(resourceURI).request().buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        final StreamsList streamsList2 = response.readEntity(StreamsList.class);
        assertEquals("List count", 200, streamsList2.getStreams().size());
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
        Response response = target(resourceURI).request().buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 200, response.getStatus());

        // Test to seal a non existent scope.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());

        // Test to seal a non existent stream.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());

        // Test to check failure.
        when(mockControllerService.sealStream(scope1, stream1)).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = target(resourceURI).request().buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 500, response.getStatus());
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

