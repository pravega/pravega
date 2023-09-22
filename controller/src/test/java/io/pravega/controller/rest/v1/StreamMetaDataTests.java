/**
 * Copyright Pravega Authors.
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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfigResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfiguration;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteReaderGroupStatus;
import io.pravega.shared.rest.RESTServer;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.RetentionConfig.TypeEnum;
import io.pravega.controller.server.rest.generated.model.TimeBasedRetention;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.getStreamForReaderGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Tests for Stream metadata REST APIs.
 */
@Slf4j
public class StreamMetaDataTests {

    protected final String scope1 = "scope1";
    protected final CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    protected final UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
    protected ControllerService mockControllerService;
    protected AuthHandlerManager authManager = null;
    protected Client client;

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private ConnectionFactory connectionFactory;
    
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final String stream3 = "stream3";
    private final String stream4 = "stream4";

    private final ScalingConfig scalingPolicyCommon = new ScalingConfig();
    private final ScalingConfig scalingPolicyCommon2 = new ScalingConfig();
    private final RetentionConfig retentionPolicyCommon = new RetentionConfig();
    private final RetentionConfig retentionPolicyCommon2 = new RetentionConfig();
    private final RetentionConfig retentionPolicyGran = new RetentionConfig();
    private final RetentionConfig retentionPolicyDateMins = new RetentionConfig();
    private final RetentionConfig retentionPolicyHoursMins = new RetentionConfig();
    private final RetentionConfig retentionPolicyOnlyHours = new RetentionConfig();
    private final RetentionConfig retentionPolicyOnlyMins = new RetentionConfig();
    private final StreamProperty streamResponseExpected = new StreamProperty();
    private final StreamProperty streamResponseExpected2 = new StreamProperty();
    private final StreamProperty streamResponseExpected3 = new StreamProperty();
    private final StreamProperty streamResponseGranExpected = new StreamProperty();
    private final StreamProperty streamResponseRetDaysMinsExpected = new StreamProperty();
    private final StreamProperty streamResponseRetHoursMinsExpected = new StreamProperty();
    private final StreamProperty streamResponseRetOnlyHoursExpected = new StreamProperty();
    private final StreamProperty streamResponseRetOnlyMinsExpected = new StreamProperty();
    private final StreamConfiguration streamConfiguration = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(123L)))
            .build();
    private final StreamConfiguration streamCfgRetTimeGranular = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(2L).plusHours(3L).plusMinutes(5L))).build();

    private final StreamConfiguration streamCfgRetTimeDaysMins = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(10L).plusMinutes(50L)))
            .build();
    private final StreamConfiguration streamCfgRetTimeHoursMins = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofHours(13L).plusMinutes(26L)))
            .build();
    private final StreamConfiguration streamCfgRetTimeOnlyHours = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofHours(16L)))
            .build();
    private final StreamConfiguration streamCfgRetTimeOnlyMins = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
            .retentionPolicy(RetentionPolicy.byTime(Duration.ofMinutes(32L)))
            .build();

    private final CreateStreamRequest createStreamRequest2 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest3 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest4 = new CreateStreamRequest();
    private final CreateStreamRequest createStreamRequest5 = new CreateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest2 = new UpdateStreamRequest();
    private final UpdateStreamRequest updateStreamRequest3 = new UpdateStreamRequest();

    private final CompletableFuture<StreamConfiguration> streamConfigFuture = CompletableFuture.
            completedFuture(streamConfiguration);
    private final CompletableFuture<StreamConfiguration> streamCfgRetTimeGranFuture = CompletableFuture.
            completedFuture(streamCfgRetTimeGranular);
    private final CompletableFuture<StreamConfiguration> streamCfgRetTimeDaysMinsFuture = CompletableFuture.
            completedFuture(streamCfgRetTimeDaysMins);
    private final CompletableFuture<StreamConfiguration> streamCfgRetTimeHoursMinsFuture = CompletableFuture.
            completedFuture(streamCfgRetTimeHoursMins);
    private final CompletableFuture<StreamConfiguration> streamCfgRetTimeOnlyHoursFuture = CompletableFuture.
            completedFuture(streamCfgRetTimeOnlyHours);
    private final CompletableFuture<StreamConfiguration> streamCfgRetTimeOnlyMinsFuture = CompletableFuture.
            completedFuture(streamCfgRetTimeOnlyMins);
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
    public void setup() throws Exception {
        mockControllerService = mock(ControllerService.class);
        serverConfig = RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort()).build();
        LocalController controller = new LocalController(mockControllerService, false, "");
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                        .build());
        restServer = new RESTServer(serverConfig, Set.of(new StreamMetadataResourceImpl(controller, mockControllerService, authManager, connectionFactory, ClientConfig.builder().build())));
        restServer.startAsync();
        restServer.awaitRunning();
        client = ClientBuilder.newClient();

        scalingPolicyCommon.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingPolicyCommon.setTargetRate(100);
        scalingPolicyCommon.setScaleFactor(2);
        scalingPolicyCommon.setMinSegments(2);

        scalingPolicyCommon2.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingPolicyCommon2.setMinSegments(2);

        retentionPolicyCommon.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyCommon.setValue(123L);
        TimeBasedRetention timeRetention = new TimeBasedRetention();
        retentionPolicyCommon.setTimeBasedRetention(timeRetention.days(123L).hours(0L).minutes(0L));

        retentionPolicyCommon2.setType(null);
        retentionPolicyCommon2.setValue(null);
        retentionPolicyCommon2.setTimeBasedRetention(null);

        streamResponseExpected.setScopeName(scope1);
        streamResponseExpected.setStreamName(stream1);
        streamResponseExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseExpected.setRetentionPolicy(retentionPolicyCommon);

        retentionPolicyGran.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyGran.setValue(0L);
        TimeBasedRetention tr = new TimeBasedRetention();
        retentionPolicyGran.setTimeBasedRetention(tr.days(2L).hours(3L).minutes(5L));
        streamResponseGranExpected.setScopeName(scope1);
        streamResponseGranExpected.setStreamName(stream1);
        streamResponseGranExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseGranExpected.setRetentionPolicy(retentionPolicyGran);

        retentionPolicyDateMins.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyDateMins.setValue(0L);
        TimeBasedRetention tr1 = new TimeBasedRetention();
        retentionPolicyDateMins.setTimeBasedRetention(tr1.days(10L).hours(0L).minutes(50L));
        streamResponseRetDaysMinsExpected.setScopeName(scope1);
        streamResponseRetDaysMinsExpected.setStreamName(stream1);
        streamResponseRetDaysMinsExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseRetDaysMinsExpected.setRetentionPolicy(retentionPolicyDateMins);

        retentionPolicyHoursMins.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyHoursMins.setValue(0L);
        TimeBasedRetention tr2 = new TimeBasedRetention();
        retentionPolicyHoursMins.setTimeBasedRetention(tr2.days(0L).hours(13L).minutes(26L));
        streamResponseRetHoursMinsExpected.setScopeName(scope1);
        streamResponseRetHoursMinsExpected.setStreamName(stream1);
        streamResponseRetHoursMinsExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseRetHoursMinsExpected.setRetentionPolicy(retentionPolicyHoursMins);

        retentionPolicyOnlyHours.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyOnlyHours.setValue(0L);
        TimeBasedRetention tr3 = new TimeBasedRetention();
        retentionPolicyOnlyHours.setTimeBasedRetention(tr3.days(0L).hours(16L).minutes(0L));
        streamResponseRetOnlyHoursExpected.setScopeName(scope1);
        streamResponseRetOnlyHoursExpected.setStreamName(stream1);
        streamResponseRetOnlyHoursExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseRetOnlyHoursExpected.setRetentionPolicy(retentionPolicyOnlyHours);

        retentionPolicyOnlyMins.setType(TypeEnum.LIMITED_DAYS);
        retentionPolicyOnlyMins.setValue(0L);
        TimeBasedRetention tr4 = new TimeBasedRetention();
        retentionPolicyOnlyMins.setTimeBasedRetention(tr4.days(0L).hours(0L).minutes(32L));
        streamResponseRetOnlyMinsExpected.setScopeName(scope1);
        streamResponseRetOnlyMinsExpected.setStreamName(stream1);
        streamResponseRetOnlyMinsExpected.setScalingPolicy(scalingPolicyCommon);
        streamResponseRetOnlyMinsExpected.setRetentionPolicy(retentionPolicyOnlyMins);

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

        // stream 4 where targetRate and scalingFactor for Scaling Policy are null
        createStreamRequest5.setStreamName(stream4);
        createStreamRequest5.setScalingPolicy(scalingPolicyCommon2);
        createStreamRequest5.setRetentionPolicy(retentionPolicyCommon);

        streamResponseExpected2.setScopeName(scope1);
        streamResponseExpected2.setStreamName(stream3);
        streamResponseExpected2.setScalingPolicy(scalingPolicyCommon);

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
    }

    @After
    public void tearDown() {
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
        connectionFactory.close();
    }

    /**
     * Test for createStream REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testCreateStream() throws ExecutionException, InterruptedException {
        String streamResourceURI = getURI() + "v1/scopes/" + scope1 + "/streams";

        // Test to create a stream which doesn't exist
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus);
        Response response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test to create a stream which doesn't exist and has no Retention Policy set.
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest4)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected2, streamResponseActual);
        response.close();

        // Test to create a stream with internal stream name
        final CreateStreamRequest streamRequest = new CreateStreamRequest();
        streamRequest.setStreamName(NameUtils.getInternalNameForStream("stream"));
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus2);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(streamRequest)).invoke();
        assertEquals("Create Stream Status", 400, response.getStatus());
        response.close();

        // Test to create a stream which doesn't exist and have Scaling Policy FIXED_NUM_SEGMENTS
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest5)).invoke();
        assertEquals("Create Stream Status", 201, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected3, streamResponseActual);
        response.close();

        // Test to create a stream that already exists
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus2);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", 409, response.getStatus());
        response.close();

        // Test for validation of create stream request object
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus3);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest2)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Create Stream Status", 500, response.getStatus());
        response.close();

        // Test create stream for non-existent scope
        when(mockControllerService.createStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(createStreamStatus4);
        response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest3)).invoke();
        assertEquals("Create Stream Status for non-existent scope", 404, response.getStatus());
        response.close();
    }

    /**
     * Test for updateStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream1;

        // Test to update an existing stream
        when(mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(updateStreamStatus);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test sending extra fields in the request object to check if json parser can handle it.
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(createStreamRequest)).invoke();
        assertEquals("Update Stream Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test to update an non-existing stream
        when(mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(updateStreamStatus2);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest2)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());
        response.close();

        // Test for validation of request object
        when(mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(updateStreamStatus3);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest3)).invoke();
        // TODO: Server should be returning 400 here, change this once issue
        // https://github.com/pravega/pravega/issues/531 is fixed.
        assertEquals("Update Stream Status", 500, response.getStatus());
        response.close();

        // Test to update stream for non-existent scope
        when(mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(updateStreamStatus4);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", 404, response.getStatus());
        response.close();
    }

    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        return request;
    }

    /**
     * Test for getStreamConfig REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testGetStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream1;
        String resourceURI2 = getURI() + "v1/scopes/" + scope1 + "/streams/" + stream2;

        // Test to get an existing stream
        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamConfigFuture);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        StreamProperty streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseExpected, streamResponseActual);
        response.close();

        // Test to get a Stream with time based Retention Config set to days, hours and mins
        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamCfgRetTimeGranFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseGranExpected, streamResponseActual);
        response.close();

        // Test to get a Stream with time based Retention Config set to days and mins
        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamCfgRetTimeDaysMinsFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseRetDaysMinsExpected, streamResponseActual);
        response.close();

        // Test to get a Stream with time based Retention Config set to hours and mins
        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamCfgRetTimeHoursMinsFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseRetHoursMinsExpected, streamResponseActual);
        response.close();

        // Test to get a Stream with time based Retention Config set to only hours ( 0 days and 0 mins)

        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamCfgRetTimeOnlyHoursFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseRetOnlyHoursExpected, streamResponseActual);
        response.close();

        // Test to get a Stream with time based Retention Config set to only mins ( 0 days and 0 hours)
        when(mockControllerService.getStream(eq(scope1), eq(stream1), anyLong())).thenReturn(streamCfgRetTimeOnlyMinsFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 200, response.getStatus());
        streamResponseActual = response.readEntity(StreamProperty.class);
        testExpectedVsActualObject(streamResponseRetOnlyMinsExpected, streamResponseActual);
        response.close();

        // Get a non-existent stream
        when(mockControllerService.getStream(eq(scope1), eq(stream2), anyLong())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, stream2);
        }));
        response = addAuthHeaders(client.target(resourceURI2).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", 404, response.getStatus());
        response.close();
    }

    /**
     * Test for deleteStream REST API
     *
     * @throws Exception
     */
    @Test(timeout = 30000)
    public void testDeleteStream() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1";

        // Test to delete a sealed stream
        when(mockControllerService.deleteStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 204, response.getStatus());
        response.close();

        // Test to delete a unsealed stream
        when(mockControllerService.deleteStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 412, response.getStatus());
        response.close();
        // Test to delete a non existent stream
        when(mockControllerService.deleteStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 404, response.getStatus());
        response.close();

        // Test to delete a stream giving an internal server error
        when(mockControllerService.deleteStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteStreamStatus.newBuilder().setStatus(DeleteStreamStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for createScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testCreateScope() throws ExecutionException, InterruptedException {
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest().scopeName(scope1);
        final String resourceURI = getURI() + "v1/scopes/";

        // Test to create a new scope.
        when(mockControllerService.createScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest))
                                                                                .invoke();
        assertEquals("Create Scope response code", 201, response.getStatus());
        response.close();

        // Test to create an existing scope.
        when(mockControllerService.createScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 409, response.getStatus());
        response.close();

        // create scope failure.
        when(mockControllerService.createScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 500, response.getStatus());
        response.close();

        // Test to create an invalid scope name.
        when(mockControllerService.createScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        createScopeRequest.setScopeName("_system");
        response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", 400, response.getStatus());
        response.close();
    }

    /**
     * Test for deleteScope REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";

        // Test to delete a scope.
        when(mockControllerService.deleteScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 204, response.getStatus());
        response.close();

        // Test to delete scope with existing streams.
        when(mockControllerService.deleteScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 412, response.getStatus());
        response.close();

        // Test to delete non-existing scope.
        when(mockControllerService.deleteScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 404, response.getStatus());
        response.close();

        // Test delete scope failure.
        when(mockControllerService.deleteScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test to retrieve a scope.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testGetScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";
        final String resourceURI2 = getURI() + "v1/scopes/scope2";

        // Test to get existent scope
        when(mockControllerService.getScope(eq(scope1), anyLong())).thenReturn(CompletableFuture.completedFuture("scope1"));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get existent scope", 200, response.getStatus());
        response.close();

        // Test to get non-existent scope
        when(mockControllerService.getScope(eq("scope2"), anyLong())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope2");
        }));
        response = addAuthHeaders(client.target(resourceURI2).request()).buildGet().invoke();
        assertEquals("Get non existent scope", 404, response.getStatus());
        response.close();

        //Test for get scope failure.
        final CompletableFuture<String> completableFuture2 = new CompletableFuture<>();
        completableFuture2.completeExceptionally(new Exception());
        when(mockControllerService.getScope(eq(scope1), anyLong())).thenReturn(completableFuture2);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get scope fail test", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for listScopes REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testlistScopes() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes";

        // Test to list scopes.
        List<String> scopesList = Arrays.asList("scope1", "scope2", "scope3");
        when(mockControllerService.listScopes(anyLong())).thenReturn(CompletableFuture.completedFuture(scopesList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Scopes response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());

        verifyScopes(response.readEntity(ScopesList.class));

        response.close();

        // Test for list scopes failure.
        final CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listScopes(anyLong())).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Scopes response code", 500, response.getStatus());
        response.close();
    }

    protected void verifyScopes(ScopesList scopesList1) {
        assertEquals(3, scopesList1.getScopes().size());
        assertEquals("scope1", scopesList1.getScopes().get(0).getScopeName());
        assertEquals("scope2", scopesList1.getScopes().get(1).getScopeName());
        assertEquals("scope3", scopesList1.getScopes().get(2).getScopeName());
    }

    /**
     * Test for listStreams REST API.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testListStreams() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams";

        final StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        final StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();

        // Test to list streams.
        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(stream1, streamConfiguration1, stream2, streamConfiguration2);

        when(mockControllerService.listStreamsInScope(eq("scope1"), anyLong())).thenReturn(CompletableFuture.completedFuture(streamsList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final StreamsList streamsList1 = response.readEntity(StreamsList.class);
        assertEquals("List count", streamsList1.getStreams().size(), 2);
        assertEquals("List element", streamsList1.getStreams().get(0).getStreamName(), "stream1");
        assertEquals("List element", streamsList1.getStreams().get(1).getStreamName(), "stream2");
        response.close();

        // Test for list streams for invalid scope.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope1"));
        when(mockControllerService.listStreamsInScope(eq("scope1"), anyLong())).thenReturn(completableFuture1);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 404, response.getStatus());
        response.close();

        // Test for list streams failure.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope(eq("scope1"), anyLong())).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 500, response.getStatus());
        response.close();

        // Test for filtering streams.
        final StreamConfiguration streamConfiguration3 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Map<String, StreamConfiguration> allStreamsList = ImmutableMap.of(stream1, streamConfiguration1, stream2,
                streamConfiguration2, NameUtils.getInternalNameForStream("stream3"), streamConfiguration3);
        when(mockControllerService.listStreamsInScope(eq("scope1"), anyLong())).thenReturn(
                CompletableFuture.completedFuture(allStreamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        StreamsList streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 2, streamsListResp.getStreams().size());
        assertEquals("List element", "stream1", streamsListResp.getStreams().get(0).getStreamName());
        assertEquals("List element", "stream2", streamsListResp.getStreams().get(1).getStreamName());
        response.close();

        response = addAuthHeaders(client.target(resourceURI).queryParam("filter_type", "showInternalStreams").request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        streamsListResp = response.readEntity(StreamsList.class);
        assertEquals("List count", 1, streamsListResp.getStreams().size());
        assertEquals("List element", NameUtils.getInternalNameForStream("stream3"),
                streamsListResp.getStreams().get(0).getStreamName());
        response.close();

        // Test for tags
        final StreamConfiguration streamConfigurationForTags = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L))).tag("testTag")
                .build();
        List<String> tagStream = new ArrayList<>();
        tagStream.add("streamForTags");
        ImmutablePair<List<String>, String> tagPair = new ImmutablePair<>(tagStream, "");
        ImmutablePair<List<String>, String> emptyPair = new ImmutablePair<>(Collections.emptyList(), "");
        when(mockControllerService.listStreamsForTag(eq("scope1"), eq("testTag"), anyString(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(tagPair))
            .thenReturn(CompletableFuture.completedFuture(emptyPair));
        when(mockControllerService.getStream(eq("scope1"), eq("streamForTags"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(streamConfigurationForTags));
        response = addAuthHeaders(client.target(resourceURI).queryParam("filter_type", "tag").queryParam("filter_value", "testTag").request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final StreamsList streamsListForTags = response.readEntity(StreamsList.class);
        assertEquals("List count", streamsListForTags.getStreams().size(), 1);
        assertEquals("List element", streamsListForTags.getStreams().get(0).getStreamName(), "streamForTags");
        response.close();

        final CompletableFuture<Pair<List<String>, String>> completableFutureForTag = new CompletableFuture<>();
        completableFutureForTag.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope1"));
        when(mockControllerService.listStreamsForTag(eq("scope1"), eq("testTag"), anyString(), anyLong())).thenReturn(completableFutureForTag);
        response = addAuthHeaders(client.target(resourceURI).queryParam("filter_type", "tag").queryParam("filter_value", "testTag").request()).buildGet().invoke();
        assertEquals("List Streams response code", 404, response.getStatus());
        response.close();

        final CompletableFuture<Pair<List<String>, String>> completableFutureForTag1 = new CompletableFuture<>();
        completableFutureForTag1.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsForTag(eq("scope1"), eq("testTag"), anyString(), anyLong())).thenReturn(completableFutureForTag1);
        response = addAuthHeaders(client.target(resourceURI).queryParam("filter_type", "tag").queryParam("filter_value", "testTag").request()).buildGet().invoke();
        assertEquals("List Streams response code", 500, response.getStatus());
        response.close();

        // Test to list large number of streams.
        streamsList = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            streamsList.put("stream" + i, streamConfiguration1);
        }
        when(mockControllerService.listStreamsInScope(eq("scope1"), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(streamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final StreamsList streamsList2 = response.readEntity(StreamsList.class);
        assertEquals("List count", 50000, streamsList2.getStreams().size());
        response.close();
    }

    /**
     * Test for updateStreamState REST API.
     */
    @Test(timeout = 30000)
    public void testUpdateStreamState() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 200, response.getStatus());
        response.close();

        // Test to seal a non existent scope.
        when(mockControllerService.sealStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());
        response.close();

        // Test to seal a non existent stream.
        when(mockControllerService.sealStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 404, response.getStatus());
        response.close();

        // Test to check failure.
        when(mockControllerService.sealStream(eq(scope1), eq(stream1), anyLong())).thenReturn(CompletableFuture.completedFuture(
                UpdateStreamStatus.newBuilder().setStatus(UpdateStreamStatus.Status.FAILURE).build()));
        streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", 500, response.getStatus());
        response.close();
    }

    /**
     * Test for getScalingEvents REST API.
     */
    @Test(timeout = 30000)
    public void testGetScalingEvents() throws Exception {
        String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/scaling-events";
        List<ScaleMetadata> scaleMetadataList = new ArrayList<>();

        /* Test to get scaling events

        There are 5 scale events in final list.
        Filter 'from' and 'to' is also tested here.
        Event1 is before 'from'
        Event2 is before 'from'
        Event3 and Event4 are between 'from' and 'to'
        Event5 is after 'to'
        Response contains 3 events : Event2 acts as reference event. Event3 and Event4 fall between 'from' and 'to'.
         */
        Segment segment1 = new Segment(0, 0, System.currentTimeMillis(), 0.00, 0.50);
        Segment segment2 = new Segment(1, 0, System.currentTimeMillis(), 0.50, 1.00);
        List<Segment> segmentList1 = Arrays.asList(segment1, segment2);
        ScaleMetadata scaleMetadata1 = new ScaleMetadata(System.currentTimeMillis() / 2, segmentList1, 0L, 0L);

        Segment segment3 = new Segment(2, 0, System.currentTimeMillis(), 0.00, 0.40);
        Segment segment4 = new Segment(3, 0, System.currentTimeMillis(), 0.40, 1.00);
        List<Segment> segmentList2 = Arrays.asList(segment3, segment4);
        ScaleMetadata scaleMetadata2 = new ScaleMetadata(1 + System.currentTimeMillis() / 2, segmentList2, 1L, 1L);

        long fromDateTime = System.currentTimeMillis();

        Segment segment5 = new Segment(4, 0, System.currentTimeMillis(), 0.00, 0.50);
        Segment segment6 = new Segment(5, 0, System.currentTimeMillis(), 0.50, 1.00);
        List<Segment> segmentList3 = Arrays.asList(segment5, segment6);
        ScaleMetadata scaleMetadata3 = new ScaleMetadata(System.currentTimeMillis(), segmentList3, 1L, 1L);

        Segment segment7 = new Segment(6, 0, System.currentTimeMillis(), 0.00, 0.25);
        Segment segment8 = new Segment(7, 0, System.currentTimeMillis(), 0.25, 1.00);
        List<Segment> segmentList4 = Arrays.asList(segment7, segment8);
        ScaleMetadata scaleMetadata4 = new ScaleMetadata(System.currentTimeMillis(), segmentList4, 1L, 1L);

        long toDateTime = System.currentTimeMillis();

        Segment segment9 = new Segment(8, 0, System.currentTimeMillis(), 0.00, 0.40);
        Segment segment10 = new Segment(9, 0, System.currentTimeMillis(), 0.40, 1.00);
        List<Segment> segmentList5 = Arrays.asList(segment9, segment10);
        ScaleMetadata scaleMetadata5 = new ScaleMetadata(toDateTime * 2, segmentList5, 1L, 1L);

        // HistoryRecords.readAllRecords returns a list of records in decreasing order
        // so we add the elements in reverse order as well to simulate that behavior
        scaleMetadataList.add(scaleMetadata5);
        scaleMetadataList.add(scaleMetadata4);
        scaleMetadataList.add(scaleMetadata3);
        scaleMetadataList.add(scaleMetadata2);
        scaleMetadataList.add(scaleMetadata1);

        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong(), anyLong());
        Response response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        List<ScaleMetadata> scaleMetadataListResponse = response.readEntity(
                new GenericType<List<ScaleMetadata>>() { });
        assertEquals("List Size", 3, scaleMetadataListResponse.size());
        scaleMetadataListResponse.forEach(data -> {
            log.warn("Here");
            data.getSegments().forEach( segment -> {
               log.debug("Checking segment number: " + segment.segmentId());
               assertTrue("Event 1 shouldn't be included", segment.segmentId() != 0L);
            });
        });

        // Test for large number of scaling events.
        scaleMetadataList.clear();
        scaleMetadataList.addAll(Collections.nCopies(50000, scaleMetadata3));
        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong(), anyLong());
        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        scaleMetadataListResponse = response.readEntity(
                new GenericType<List<ScaleMetadata>>() { });
        assertEquals("List Size", 50000, scaleMetadataListResponse.size());

        // Test for getScalingEvents for invalid scope/stream.
        final CompletableFuture<List<ScaleMetadata>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "stream1"));
        doAnswer(x -> completableFuture1)
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).
                queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 404, response.getStatus());

        // Test for getScalingEvents for bad request.
        // 1. Missing query parameters are validated here.
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        // 2. from > to is tested here.
        doAnswer(x -> CompletableFuture.completedFuture(scaleMetadataList))
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI).queryParam("from", fromDateTime * 2).
                queryParam("to", fromDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 400, response.getStatus());

        // Test for getScalingEvents failure.
        final CompletableFuture<List<ScaleMetadata>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        doAnswer(x -> completableFuture)
                .when(mockControllerService).getScaleRecords(anyString(), anyString(), anyLong(), anyLong(), anyLong());

        response = addAuthHeaders(client.target(resourceURI)
                                        .queryParam("from", fromDateTime)
                                        .queryParam("to", toDateTime).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 500, response.getStatus());
    }

    /**
     * Test for listReaderGroups REST API.
     */
    @Test(timeout = 30000)
    public void testListReaderGroups() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups";

        final StreamConfiguration streamconf1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration streamconf2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration readerGroup1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final StreamConfiguration readerGroup2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        // Fetch reader groups list.
        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(stream1, streamconf1, stream2,
                streamconf2, getStreamForReaderGroup("readerGroup1"), readerGroup1,
                getStreamForReaderGroup("readerGroup2"), readerGroup2);
        when(mockControllerService.listStreamsInScope(eq(scope1), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(     streamsList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final ReaderGroupsList readerGroupsList1 = response.readEntity(ReaderGroupsList.class);
        assertEquals("List count", readerGroupsList1.getReaderGroups().size(), 2);
        assertEquals("List element", readerGroupsList1.getReaderGroups().get(0).getReaderGroupName(),
                "readerGroup1");
        assertEquals("List element", readerGroupsList1.getReaderGroups().get(1).getReaderGroupName(),
                "readerGroup2");
        response.close();

        // Test for list reader groups for non-existing scope.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture1 = new CompletableFuture<>();
        completableFuture1.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope1"));
        when(mockControllerService.listStreamsInScope(eq(scope1), anyLong())).thenReturn(completableFuture1);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 404, response.getStatus());
        response.close();

        // Test for list reader groups failure.
        final CompletableFuture<Map<String, StreamConfiguration>> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception());
        when(mockControllerService.listStreamsInScope(eq(scope1), anyLong())).thenReturn(completableFuture);
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 500, response.getStatus());
        response.close();

        // Test to list large number of reader groups.
        streamsList = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            streamsList.put(getStreamForReaderGroup("readerGroup" + i), readerGroup1);
        }
        when(mockControllerService.listStreamsInScope(eq(scope1), anyLong())).thenReturn(
                CompletableFuture.completedFuture(streamsList));
        response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 200, response.getStatus());
        assertTrue(response.bufferEntity());
        final ReaderGroupsList readerGroupsList = response.readEntity(ReaderGroupsList.class);
        assertEquals("List count", 50000, readerGroupsList.getReaderGroups().size());
        response.close();
    }

    @Test(timeout = 30000)
    public void testGetReaderGroup() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups/readergroup1";

        when(mockControllerService.getExecutor()).thenReturn(connectionFactory.getInternalExecutor());
        when(mockControllerService.getCurrentSegments(anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Verify that a ReaderGroupNotFoundException is generated as the segments are null.
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", 404, response.getStatus());
        verify(mockControllerService, times(1)).getCurrentSegments(anyString(), anyString(), anyLong());
    }

    /**
     * Test for deleteReaderGroup REST API
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(timeout = 30000)
    public void testDeleteReaderGroup() {
        final String scope = "scope";
        final String streamName = "stream1";
        final String rgName = "readergroup1";
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups/readergroup1";

        when(mockControllerService.getExecutor()).thenReturn(connectionFactory.getInternalExecutor());

        StreamInfo info = StreamInfo.newBuilder().setScope(scope).setStream(streamName).build();
        SegmentId segment1 = SegmentId.newBuilder().setSegmentId(1).setStreamInfo(info).build();
        SegmentRange segmentRange1 = SegmentRange.newBuilder().setSegmentId(segment1)
                                                            .setMinKey(0.1).setMaxKey(0.3).build();

        List<SegmentRange> segmentsList = new ArrayList<>(1);
        segmentsList.add(segmentRange1);
        when(mockControllerService.getCurrentSegments(anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(segmentsList));

        final io.pravega.client.segment.impl.Segment seg0 = new io.pravega.client.segment.impl.Segment(scope, streamName, 0L);
        final io.pravega.client.segment.impl.Segment seg1 = new io.pravega.client.segment.impl.Segment(scope, streamName, 1L);
        ImmutableMap<io.pravega.client.segment.impl.Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of(scope, streamName),
                new StreamCutImpl(Stream.of(scope, streamName), startStreamCut));
        ImmutableMap<io.pravega.client.segment.impl.Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(Stream.of(scope, streamName),
                new StreamCutImpl(Stream.of(scope, streamName), endStreamCut));
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                                                      .automaticCheckpointIntervalMillis(30000L)
                                                      .groupRefreshTimeMillis(20000L)
                                                      .maxOutstandingCheckpointRequest(2)
                                                      .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                                                      .startingStreamCuts(startSC)
                                                      .endingStreamCuts(endSC).build();
        ReaderGroupConfig config = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 0L);

        ReaderGroupConfiguration expectedConfig = ModelHelper.decode(scope, rgName, config);

        when(mockControllerService.getReaderGroupConfig(anyString(), anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(ReaderGroupConfigResponse.newBuilder()
                                                                           .setStatus(ReaderGroupConfigResponse.Status.SUCCESS)
                                                                           .setConfig(expectedConfig)
                                                                           .build()));
        when(mockControllerService.deleteReaderGroup(anyString(), anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(
                DeleteReaderGroupStatus.newBuilder().setStatus(DeleteReaderGroupStatus.Status.SUCCESS).build()));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete reader group response code", 204, response.getStatus());

        when(mockControllerService.deleteReaderGroup(anyString(), anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(
                        DeleteReaderGroupStatus.newBuilder().setStatus(DeleteReaderGroupStatus.Status.RG_NOT_FOUND).build()));
        response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete reader group response code", 404, response.getStatus());
        response.close();
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
        assertEquals("StreamConfig: Retention Policy",
                expected.getRetentionPolicy(),
                actual.getRetentionPolicy());
    }

    protected String getURI() {
        return "http://localhost:" + serverConfig.getPort() + "/";
    }
}

