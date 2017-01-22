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
import com.emc.pravega.controller.server.rest.resources.StreamMetaDataResourceImpl;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
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
import java.util.concurrent.Future;

import static com.emc.pravega.stream.ScalingPolicy.Type.FIXED_NUM_SEGMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamMetaDataTests extends JerseyTest {


    ControllerService mockControllerService;
    StreamMetadataStore mockStreamStore;
    StreamMetaDataResourceImpl streamMetaDataResource;
    Future<Response> response;
    StreamResponse streamResponseActual;

    private final String stream1 = "stream1";
    private final String scope1 = "scope1";
    private final String resourceURI = "v1/scopes/" + scope1 + "/streams/" + stream1;
    private final String streamResourceURI = "v1/scopes/" + scope1 + "/streams";

    private final ScalingPolicyCommon scalingPolicyCommon = new ScalingPolicyCommon(
            ScalingPolicyCommon.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
    private final RetentionPolicyCommon retentionPolicyCommon = new RetentionPolicyCommon(123L);
    private final StreamResponse streamResponseExpected = new StreamResponse(
            new StreamProperty(scope1, stream1, scalingPolicyCommon, retentionPolicyCommon));
    private final StreamConfiguration streamConfiguration = new StreamConfigurationImpl(scope1, stream1,
            new ScalingPolicy(FIXED_NUM_SEGMENTS, 100L, 2, 2), new RetentionPolicy(123L));

    private final CreateStreamRequest createStreamRequest = new CreateStreamRequest(
            stream1, scalingPolicyCommon, retentionPolicyCommon);
    private final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest(
            scalingPolicyCommon, retentionPolicyCommon);

    private final CompletableFuture<StreamConfiguration> streamConfigFuture = CompletableFuture.supplyAsync(
            () -> streamConfiguration);
    private final CompletableFuture<CreateStreamStatus> createStreamStatus = CompletableFuture.supplyAsync(
            () -> CreateStreamStatus.SUCCESS);
    private CompletableFuture<UpdateStreamStatus> updateStreamStatus = CompletableFuture.supplyAsync(
            () -> UpdateStreamStatus.SUCCESS);

    @Override
    protected Application configure() {
        mockControllerService = mock(ControllerService.class);
        mockStreamStore = mock(StreamMetadataStore.class);
        streamMetaDataResource = new StreamMetaDataResourceImpl(mockControllerService);

        return new ResourceConfig()
                .register(streamMetaDataResource)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(mockControllerService).to(ControllerService.class);
                    }
                });
    }

    @Test
    public void testCreateStream() throws ExecutionException, InterruptedException {
        when(mockControllerService.createStream(any(), anyLong())).thenReturn(createStreamStatus);

        response = target(streamResourceURI).request().async().
                post(Entity.json(createStreamRequest));
        streamResponseActual = response.get().readEntity(StreamResponse.class);

        assertEquals("Create Stream Status", 201, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
    }

    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        when(mockControllerService.alterStream(any())).thenReturn(updateStreamStatus);

        response = target(resourceURI).request().async().
                put(Entity.json(updateStreamRequest));
        streamResponseActual = response.get().readEntity(StreamResponse.class);

        assertEquals("Update Stream Status", 201, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
    }

    @Test
    public void testGetStreamConfig() throws ExecutionException, InterruptedException {
        when(mockControllerService.getStreamStore()).thenReturn(mockStreamStore);
        when(mockStreamStore.getConfiguration(stream1)).thenReturn(streamConfigFuture);

        response = target(resourceURI).request().async().get();
        streamResponseActual = response.get().readEntity(StreamResponse.class);

        assertEquals("Get Stream Config Status", 200, response.get().getStatus());
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
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
