/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.rest.v1;

import com.emc.pravega.controller.server.rest.contract.common.RetentionPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.common.ScalingPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.response.StreamProperty;
import com.emc.pravega.controller.server.rest.contract.response.StreamResponse;
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
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

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
    StreamResponse streamResponseActual;

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final String scope1 = "scope1";
    private final String resourceURI = "v1/scopes/" + scope1 + "/streams/" + stream1;
    private final String resourceURI2 = "v1/scopes/" + scope1 + "/streams/" + stream2;
    private final String streamResourceURI = "v1/scopes/" + scope1 + "/streams";
    private final ScalingPolicyCommon scalingPolicyCommon = new ScalingPolicyCommon(
            ScalingPolicyCommon.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
    private final RetentionPolicyCommon retentionPolicyCommon = new RetentionPolicyCommon(123L);
    private final RetentionPolicyCommon retentionPolicyCommon2 = new RetentionPolicyCommon(null);
    private final StreamResponse streamResponseExpected = new StreamResponse(
            new StreamProperty(scope1, stream1, scalingPolicyCommon, retentionPolicyCommon));
    private final StreamConfiguration streamConfiguration = new StreamConfigurationImpl(scope1, stream1,
            new ScalingPolicy(FIXED_NUM_SEGMENTS, 100, 2, 2), new RetentionPolicy(123L));

    private final CreateStreamRequest createStreamRequest = new CreateStreamRequest(
            stream1, scalingPolicyCommon, retentionPolicyCommon);
    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest(
            stream1, scalingPolicyCommon, retentionPolicyCommon2);
    private final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest(
            scalingPolicyCommon, retentionPolicyCommon);
    private final UpdateStreamRequest updateStreamRequest2 = new UpdateStreamRequest(
            scalingPolicyCommon, retentionPolicyCommon);

    private final UpdateStreamRequest updateStreamRequest3 = new UpdateStreamRequest(
            scalingPolicyCommon, retentionPolicyCommon2);

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

    /**
     * Configure resource class.
     * @return JAX-RS application
     */
    @Override
    protected Application configure() {
        mockControllerService = mock(ControllerService.class);
        mockStreamStore = mock(StreamMetadataStore.class);
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
        streamResponseActual = response.get().readEntity(StreamResponse.class);
        assertEquals("Create Stream Status", 201, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to create a stream that already exists
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus2);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest));
        assertEquals("Create Stream Status", 409, response.get().getStatus());

        // Test for validation of create stream request object
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus3);
        response = target(streamResourceURI).request().async().post(Entity.json(createStreamRequest2));
        assertEquals("Create Stream Status", 400, response.get().getStatus());
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
        streamResponseActual = response.get().readEntity(StreamResponse.class);
        assertEquals("Update Stream Status", 201, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Test to update an non-existing stream
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus2);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest2));
        assertEquals("Update Stream Status", 404, response.get().getStatus());

        // Test for validation of request object
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus3);
        response = target(resourceURI).request().async().put(Entity.json(updateStreamRequest3));
        assertEquals("Update Stream Status", 400, response.get().getStatus());
    }

    /**
     * Test for getStreamConfig REST API
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testGetStreamConfig() throws ExecutionException, InterruptedException {

        // Test to get an existing stream
        when(mockControllerService.getStreamConfiguration(scope1, stream1)).thenReturn(streamConfigFuture);
        response = target(resourceURI).request().async().get();
        streamResponseActual = response.get().readEntity(StreamResponse.class);
        assertEquals("Get Stream Config Status", 200, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);

        // Get a non-existent stream
        when(mockControllerService.getStreamConfiguration(scope1, stream2)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new DataNotFoundException("Stream Not Found");
        }));
        response = target(resourceURI2).request().async().get();
        streamResponseActual = response.get().readEntity(StreamResponse.class);
        assertEquals("Get Stream Config Status", 404, response.get().getStatus());
    }

    private static void testExpectedVsActualObject(StreamResponse expected, StreamResponse actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals("StreamConfig: Scope Name ", expected.getStream().getScope(), actual.getStream().getScope());
        assertEquals("StreamConfig: Stream Name ",
                expected.getStream().getStreamName(), actual.getStream().getStreamName());
        assertEquals("StreamConfig: Scaling Policy: Type",
                expected.getStream().getScalingPolicy().getType(), actual.getStream().getScalingPolicy().getType());
        assertEquals("StreamConfig: Scaling Policy: Target Rate",
                expected.getStream().getScalingPolicy().getTargetRate(),
                actual.getStream().getScalingPolicy().getTargetRate());
        assertEquals("StreamConfig: Scaling Policy: Scale Factor",
                expected.getStream().getScalingPolicy().getScaleFactor(),
                actual.getStream().getScalingPolicy().getScaleFactor());
        assertEquals("StreamConfig: Scaling Policy: MinNumSegments",
                expected.getStream().getScalingPolicy().getMinNumSegments(),
                actual.getStream().getScalingPolicy().getMinNumSegments());
        assertEquals("StreamConfig: Retention Policy: MinNumSegments",
                expected.getStream().getRetentionPolicy().getRetentionTimeMillis(),
                actual.getStream().getRetentionPolicy().getRetentionTimeMillis());
    }
}
