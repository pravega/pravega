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
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.control.impl.ReaderGroupConfigRejectedException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.ControllerToBucketMappingRequest.BucketType;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static io.pravega.test.common.AssertExtensions.assertMapEquals;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for LocalController.
 */
@Slf4j
public class LocalControllerTest extends ThreadPooledTestSuite {

    boolean authEnabled = false;

    private ControllerService mockControllerService;
    private LocalController testController;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setup() {
        this.mockControllerService = mock(ControllerService.class);
        this.testController = new LocalController(this.mockControllerService, authEnabled, "secret");
    }

    @Test(timeout = 10000)
    public void testListScopes() {
        when(this.mockControllerService.listScopes(any(), anyInt(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(new ImmutablePair<>(Lists.newArrayList("a", "b", "c"),
                        "last")));
        when(this.mockControllerService.listScopes(eq("last"), anyInt(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(new ImmutablePair<>(Collections.emptyList(), "last")));
        AsyncIterator<String> iterator = this.testController.listScopes();
        assertEquals(iterator.getNext().join(), "a");
        assertEquals(iterator.getNext().join(), "b");
        assertEquals(iterator.getNext().join(), "c");
        Assert.assertNull(iterator.getNext().join());
    }

    @Test(timeout = 10000)
    public void testGetControllerToBucketMapping() {
        Map<String, Set<Integer>> map = ImmutableMap.of("Controller1", Set.of(1, 2, 3));
        when(this.mockControllerService.getControllerToBucketMapping(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(map));
        assertMapEquals("ControllerToBucketMapping", map,
                mockControllerService.getControllerToBucketMapping(BucketType.RetentionService, 0L).join());
    }
    
    @Test(timeout = 10000)
    public void testCreateScope() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.createScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createScope("scope").join());

        when(this.mockControllerService.getScope(eq("scope"), anyLong())).thenReturn(
                CompletableFuture.completedFuture("scope"));
        Assert.assertTrue(this.testController.checkScopeExists("scope").join());

        when(this.mockControllerService.getScope(eq("scope2"), anyLong())).thenReturn(
                Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "data not found")));
        Assert.assertFalse(this.testController.checkScopeExists("scope2").join());
        
        when(this.mockControllerService.createScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        Assert.assertFalse(this.testController.createScope("scope").join());

        when(this.mockControllerService.createScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.INVALID_SCOPE_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteScope("scope").join());

        when(this.mockControllerService.deleteScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteScope("scope").join());

        when(this.mockControllerService.deleteScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        assertThrows("Expected IllegalStateException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof IllegalStateException);

        when(this.mockControllerService.deleteScope(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testCreateStream() {
        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.getStream(eq("scope"), eq("stream"), anyLong())).thenReturn(
                CompletableFuture.completedFuture(StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build()));
        Assert.assertTrue(this.testController.checkStreamExists("scope", "stream").join());
        when(this.mockControllerService.getStream(eq("scope"), eq("notExist"), anyLong())).thenReturn(
                Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "stream not found")));
        Assert.assertFalse(this.testController.checkStreamExists("scope", "notExist").join());

        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.STREAM_EXISTS).build()));
        Assert.assertFalse(this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.INVALID_STREAM_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createInternalStream(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createInternalStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testCreateReaderGroup() throws ExecutionException, InterruptedException {
        final Segment seg0 = new Segment("scope", "stream1", 0L);
        final Segment seg1 = new Segment("scope", "stream1", 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of("scope", "stream1"),
                new StreamCutImpl(Stream.of("scope", "stream1"), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(Stream.of("scope", "stream1"),
                new StreamCutImpl(Stream.of("scope", "stream1"), endStreamCut));
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC).build();
        final ReaderGroupConfig config = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 0L);
        StreamMetadataTasks mockStreamMetaTasks = mock(StreamMetadataTasks.class);
        final String scope = "scope";
        final String rgName = "subscriber";
        when(this.mockControllerService.getStreamMetadataTasks()).thenReturn(mockStreamMetaTasks);
        Controller.ReaderGroupConfiguration expectedConfig = ModelHelper.decode(scope, rgName, config);
        when(mockStreamMetaTasks.createReaderGroupInternal(anyString(), any(), any(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                        .setConfig(expectedConfig)
                        .setStatus(Controller.CreateReaderGroupResponse.Status.SUCCESS).build()));
        ReaderGroupConfig responseCfg = this.testController.createReaderGroup(scope, rgName, config).join();
        Assert.assertEquals(UUID.fromString(expectedConfig.getReaderGroupId()), responseCfg.getReaderGroupId());
        Assert.assertEquals(expectedConfig.getRetentionType(), responseCfg.getRetentionType().ordinal());
        Assert.assertEquals(expectedConfig.getGeneration(), responseCfg.getGeneration());
        Assert.assertEquals(expectedConfig.getGroupRefreshTimeMillis(), responseCfg.getGroupRefreshTimeMillis());
        Assert.assertEquals(expectedConfig.getAutomaticCheckpointIntervalMillis(), responseCfg.getAutomaticCheckpointIntervalMillis());
        Assert.assertEquals(expectedConfig.getMaxOutstandingCheckpointRequest(), responseCfg.getMaxOutstandingCheckpointRequest());

        when(mockStreamMetaTasks.createReaderGroupInternal(anyString(), any(), any(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                                .setConfig(expectedConfig)
                                .setStatus(Controller.CreateReaderGroupResponse.Status.FAILURE).build()));

        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof ControllerFailureException);

        when(mockStreamMetaTasks.createReaderGroupInternal(anyString(), any(), any(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                        .setConfig(expectedConfig)
                        .setStatus(Controller.CreateReaderGroupResponse.Status.INVALID_RG_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(mockStreamMetaTasks.createReaderGroupInternal(anyString(), any(), any(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                        .setConfig(expectedConfig)
                        .setStatus(Controller.CreateReaderGroupResponse.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(mockStreamMetaTasks.createReaderGroupInternal(anyString(), any(), any(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testGetReaderGroupConfig() throws ExecutionException, InterruptedException {
        final String scope = "scope";
        final String streamName = "stream1";
        final Segment seg0 = new Segment(scope, streamName, 0L);
        final Segment seg1 = new Segment(scope, streamName, 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of(scope, streamName),
                new StreamCutImpl(Stream.of(scope, streamName), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
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
        final String rgName = "subscriber";
        Controller.ReaderGroupConfiguration expectedConfig = ModelHelper.decode(scope, rgName, config);
        when(this.mockControllerService.getReaderGroupConfig(anyString(), anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ReaderGroupConfigResponse.newBuilder()
                        .setStatus(Controller.ReaderGroupConfigResponse.Status.SUCCESS)
                        .setConfig(expectedConfig)
                        .build()));

        Assert.assertEquals(this.testController.getReaderGroupConfig(scope, rgName).join()
                        .getAutomaticCheckpointIntervalMillis(),
                        config.getAutomaticCheckpointIntervalMillis());

        when(this.mockControllerService.getReaderGroupConfig(anyString(), anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ReaderGroupConfigResponse.newBuilder()
                        .setStatus(Controller.ReaderGroupConfigResponse.Status.FAILURE)
                        .setConfig(Controller.ReaderGroupConfiguration.getDefaultInstance())
                        .build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.getReaderGroupConfig("scope", "subscriber").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.getReaderGroupConfig(anyString(), anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ReaderGroupConfigResponse.newBuilder()
                        .setStatus(Controller.ReaderGroupConfigResponse.Status.RG_NOT_FOUND)
                        .setConfig(Controller.ReaderGroupConfiguration.getDefaultInstance())
                        .build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.getReaderGroupConfig("scope", "subscriber").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.getReaderGroupConfig(eq("scope"), eq("subscriber"), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ReaderGroupConfigResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.getReaderGroupConfig("scope", "subscriber").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testDeleteReaderGroup() throws ExecutionException, InterruptedException {
        final  UUID someUUID = UUID.randomUUID();
        when(this.mockControllerService.deleteReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteReaderGroupStatus.newBuilder()
                        .setStatus(Controller.DeleteReaderGroupStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteReaderGroup("scope", "subscriber", someUUID).join());

        when(this.mockControllerService.deleteReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteReaderGroupStatus.newBuilder()
                        .setStatus(Controller.DeleteReaderGroupStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteReaderGroup("scope", "subscriber", someUUID).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteReaderGroupStatus.newBuilder()
                        .setStatus(Controller.DeleteReaderGroupStatus.Status.RG_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.deleteReaderGroup("scope", "stream", someUUID).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.deleteReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteReaderGroupStatus.newBuilder()
                        .setStatus(Controller.DeleteReaderGroupStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteReaderGroup("scope", "subscriber", someUUID).join());
        
        when(this.mockControllerService.deleteReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteReaderGroupStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteReaderGroup("scope", "subscriber", someUUID).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testUpdateReaderGroup() throws ExecutionException, InterruptedException {
        final Segment seg0 = new Segment("scope", "stream1", 0L);
        final Segment seg1 = new Segment("scope", "stream1", 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of("scope", "stream1"),
                new StreamCutImpl(Stream.of("scope", "stream1"), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(Stream.of("scope", "stream1"),
                new StreamCutImpl(Stream.of("scope", "stream1"), endStreamCut));
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC).build();
        ReaderGroupConfig config = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 0L);
        when(this.mockControllerService.updateReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateReaderGroupResponse.newBuilder()
                        .setStatus(Controller.UpdateReaderGroupResponse.Status.SUCCESS).setGeneration(1L).build()));
        Assert.assertNotNull(this.testController.updateReaderGroup("scope", "subscriber", config).join());

        when(this.mockControllerService.updateReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateReaderGroupResponse.newBuilder()
                        .setStatus(Controller.UpdateReaderGroupResponse.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.updateReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateReaderGroupResponse.newBuilder()
                        .setStatus(Controller.UpdateReaderGroupResponse.Status.INVALID_CONFIG).build()));
        assertThrows("Expected ReaderGroupConfigRejectedException",
                () -> this.testController.updateReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof ReaderGroupConfigRejectedException);

        when(this.mockControllerService.updateReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateReaderGroupResponse.newBuilder()
                        .setStatus(Controller.UpdateReaderGroupResponse.Status.RG_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateReaderGroup(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateReaderGroupResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateReaderGroup("scope", "subscriber", config).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testUpdateSubscriberStreamCut() throws ExecutionException, InterruptedException {
        UUID someId = UUID.randomUUID();
        StreamCut streamCut = new StreamCutImpl(new StreamImpl("scope", "stream"), Collections.emptyMap());
        when(this.mockControllerService.updateSubscriberStreamCut(anyString(), anyString(), anyString(), any(),
                anyLong(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateSubscriberStatus.newBuilder()
                        .setStatus(Controller.UpdateSubscriberStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.updateSubscriberStreamCut("scope", "stream", "subscriber", someId, 0L, streamCut).join());

        when(this.mockControllerService.updateSubscriberStreamCut(anyString(), anyString(), anyString(), any(),
                anyLong(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateSubscriberStatus.newBuilder()
                        .setStatus(Controller.UpdateSubscriberStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateSubscriberStreamCut("scope", "stream", "subscriber", someId, 0L, streamCut).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.updateSubscriberStreamCut(anyString(), anyString(), anyString(), any(),
                anyLong(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateSubscriberStatus.newBuilder()
                        .setStatus(Controller.UpdateSubscriberStatus.Status.STREAM_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateSubscriberStreamCut("scope", "stream", "subscriber",
                        someId, 0L, streamCut).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateSubscriberStreamCut(anyString(), anyString(), anyString(), any(),
                anyLong(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateSubscriberStatus.newBuilder()
                        .setStatus(Controller.UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateSubscriberStreamCut("scope", "stream", "subscriber", someId, 0L, streamCut).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateSubscriberStreamCut(anyString(), anyString(), anyString(), any(),
                anyLong(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateSubscriberStatus.newBuilder()
                        .setStatus(Controller.UpdateSubscriberStatus.Status.GENERATION_MISMATCH).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateSubscriberStreamCut("scope", "stream", "subscriber", someId, 0L, streamCut).join(),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test(timeout = 10000)
    public void testListSubscribers() throws ExecutionException, InterruptedException {
        List<String> subscriberList = new ArrayList<>(3);
        subscriberList.add("sub1");
        subscriberList.add("sub2");
        subscriberList.add("sub3");
        Controller.SubscribersResponse result = Controller.SubscribersResponse.newBuilder()
                .addAllSubscribers(subscriberList).setStatus(Controller.SubscribersResponse.Status.SUCCESS).build();

        when(this.mockControllerService.listSubscribers(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(result));
        List<String> returnedSubscribers = this.testController.listSubscribers("scope", "stream").join();
        Assert.assertEquals(3, returnedSubscribers.size());
        Assert.assertTrue(returnedSubscribers.contains("sub1"));
        Assert.assertTrue(returnedSubscribers.contains("sub2"));
        Assert.assertTrue(returnedSubscribers.contains("sub3"));

        List<String> emptyList = new ArrayList<String>();
        result = Controller.SubscribersResponse.newBuilder()
                .addAllSubscribers(emptyList).setStatus(Controller.SubscribersResponse.Status.SUCCESS).build();
        when(this.mockControllerService.listSubscribers(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(result));
        returnedSubscribers = this.testController.listSubscribers("scope", "stream").join();
        Assert.assertEquals(0, returnedSubscribers.size());

        result = Controller.SubscribersResponse.newBuilder()
                .addAllSubscribers(emptyList).setStatus(Controller.SubscribersResponse.Status.STREAM_NOT_FOUND).build();
        when(this.mockControllerService.listSubscribers(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(result));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.listSubscribers("scope", "unknownstream").join(),
                ex -> ex instanceof IllegalArgumentException);

    }

    @Test(timeout = 10000)
    public void testSealStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.sealStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.sealStream("scope", "stream").join());

        when(this.mockControllerService.sealStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.sealStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        assertThrows("Expected InvalidStreamException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof InvalidStreamException);

        when(this.mockControllerService.sealStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected InvalidStreamException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof InvalidStreamException);

        when(this.mockControllerService.sealStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testDeleteStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteStream("scope", "stream").join());

        when(this.mockControllerService.deleteStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteStream("scope", "stream").join());

        when(this.mockControllerService.deleteStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.deleteStream(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testScaleStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.checkScale(anyString(), anyString(), anyInt(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleStatusResponse.newBuilder()
                        .setStatus(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS).build()));
        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.STARTED).build()));
        Assert.assertTrue(this.testController.scaleStream(new StreamImpl("scope", "stream"),
                new ArrayList<>(), new HashMap<>(), executorService()).getFuture().join());

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED).build()));
        Assert.assertFalse(this.testController.scaleStream(new StreamImpl("scope", "stream"),
                new ArrayList<>(), new HashMap<>(), executorService()).getFuture().join());

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.startScale(new StreamImpl("scope", "stream"),
                        new ArrayList<>(), new HashMap<>()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.startScale(new StreamImpl("scope", "stream"),
                        new ArrayList<>(), new HashMap<>()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testGetSegmentsBetween() throws ExecutionException, InterruptedException {
        List<StreamSegmentRecord> list = new ArrayList<>();
        when(this.mockControllerService.getSegmentsBetweenStreamCuts(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(list));
        Assert.assertTrue(Futures.await(this.testController.getSegments(new StreamCutImpl(new StreamImpl("scope", "stream"), Collections.emptyMap()),
                new StreamCutImpl(new StreamImpl("scope", "stream"), Collections.emptyMap()))));
    }

    @Test(timeout = 10000)
    public void testCreateKeyValueTable() {
        val kvtConfig = KeyValueTableConfiguration.builder().partitionCount(1).primaryKeyLength(4).secondaryKeyLength(4).build();
        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createKeyValueTable("scope", "kvtable",
                kvtConfig).join());

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.TABLE_EXISTS).build()));
        Assert.assertFalse(this.testController.createKeyValueTable("scope", "kvtable",
                kvtConfig).join());

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        kvtConfig).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.INVALID_TABLE_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        kvtConfig).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        kvtConfig).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createKeyValueTable("scope", "kvtable1",
                        kvtConfig).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testGetCurrentSegmentsKeyValueTable() throws Exception {
        Controller.StreamInfo info = Controller.StreamInfo.newBuilder().setScope("scope").setStream("kvtable").build();
        Controller.SegmentId segment1 = Controller.SegmentId.newBuilder().setSegmentId(1).setStreamInfo(info).build();
        Controller.SegmentId segment2 = Controller.SegmentId.newBuilder().setSegmentId(2).setStreamInfo(info).build();
        Controller.SegmentId segment3 = Controller.SegmentId.newBuilder().setSegmentId(3).setStreamInfo(info).build();

        Controller.SegmentRange segmentRange1 = Controller.SegmentRange.newBuilder().setSegmentId(segment1).setMinKey(0.1).setMaxKey(0.3).build();
        Controller.SegmentRange segmentRange2 = Controller.SegmentRange.newBuilder().setSegmentId(segment2).setMinKey(0.4).setMaxKey(0.6).build();
        Controller.SegmentRange segmentRange3 = Controller.SegmentRange.newBuilder().setSegmentId(segment3).setMinKey(0.7).setMaxKey(1.0).build();

        List<Controller.SegmentRange> segmentsList = new ArrayList<Controller.SegmentRange>(3);
        segmentsList.add(segmentRange1);
        segmentsList.add(segmentRange2);
        segmentsList.add(segmentRange3);

        when(this.mockControllerService.getCurrentSegmentsKeyValueTable(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(segmentsList));
        KeyValueTableSegments segments = this.testController.getCurrentSegmentsForKeyValueTable("scope", "kvtable").get();
        assertEquals(3, segments.getSegments().size());
    }

    @Test(timeout = 10000)
    public void testListKeyValueTable() throws Exception {
        List<String> tablelist = new ArrayList<String>();
        tablelist.add("kvtable1");
        Pair<List<String>, String> listOfKVTables = new ImmutablePair<>(tablelist, "");
        when(this.mockControllerService.listKeyValueTables(anyString(), anyString(), anyInt(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(listOfKVTables));
        KeyValueTableInfo info = this.testController.listKeyValueTables("scope").getNext().get();
        assertEquals("kvtable1", info.getKeyValueTableName());
    }

    @Test(timeout = 10000)
    public void testGetKeyValueTableConfiguration() {
        KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        when(this.mockControllerService.getKeyValueTableConfiguration(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.KeyValueTableConfigResponse.newBuilder()
                        .setStatus(Controller.KeyValueTableConfigResponse.Status.SUCCESS)
                        .setConfig(ModelHelper.decode("scope", "kvtable1", config)).build()));
        KeyValueTableConfiguration responseConfig = this.testController.getKeyValueTableConfiguration("scope", "kvtable1").join();
        assertEquals(config.getPartitionCount(), responseConfig.getPartitionCount());
        assertEquals(config.getPrimaryKeyLength(), responseConfig.getPrimaryKeyLength());
        assertEquals(config.getSecondaryKeyLength(), responseConfig.getSecondaryKeyLength());

        when(this.mockControllerService.getKeyValueTableConfiguration(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.KeyValueTableConfigResponse.newBuilder()
                        .setStatus(Controller.KeyValueTableConfigResponse.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.getKeyValueTableConfiguration("scope", "kvtable2").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.getKeyValueTableConfiguration(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.KeyValueTableConfigResponse.newBuilder()
                        .setStatus(Controller.KeyValueTableConfigResponse.Status.TABLE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.getKeyValueTableConfiguration("scope", "kvtable3").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.getKeyValueTableConfiguration(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.KeyValueTableConfigResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.getKeyValueTableConfiguration("scope", "kvtable4").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testDeleteKeyValueTable() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteKeyValueTable(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteKeyValueTable("scope", "kvtable1").join());

        when(this.mockControllerService.deleteKeyValueTable(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteKeyValueTable("scope", "kvtable2").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteKeyValueTable(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.TABLE_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteKeyValueTable("scope", "kvtable3").join());

        when(this.mockControllerService.deleteKeyValueTable(any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteKeyValueTable("scope", "kvtable4").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testDeleteScopeRecursive() {
        when(this.mockControllerService.deleteScopeRecursive(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteScopeRecursive("testScope").join());

        when(this.mockControllerService.deleteScopeRecursive(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteScopeRecursive("testScope").join());

        when(this.mockControllerService.deleteScopeRecursive(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScopeRecursive("testScope").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteScopeRecursive(any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScopeRecursive("testScope").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test(timeout = 10000)
    public void testGetOrRefreshDelegationToken() {
        String token = this.testController.getOrRefreshDelegationTokenFor("scope", "stream", null).join();
         if (this.authEnabled) {
             assertNotNull(token);
         } else {
             assertEquals("", token);
         }
    }

    @Test(timeout = 10000)
    public void testGetCurrentSegments() {
        Controller.StreamInfo info = Controller.StreamInfo.newBuilder().setScope("scope").setStream("stream").build();
        Controller.SegmentId segment1 = Controller.SegmentId.newBuilder().setSegmentId(1).setStreamInfo(info).build();
        Controller.SegmentId segment2 = Controller.SegmentId.newBuilder().setSegmentId(2).setStreamInfo(info).build();
        Controller.SegmentId segment3 = Controller.SegmentId.newBuilder().setSegmentId(3).setStreamInfo(info).build();

        Controller.SegmentRange segmentRange1 = Controller.SegmentRange.newBuilder().setSegmentId(segment1).setMinKey(0.1).setMaxKey(0.3).build();
        Controller.SegmentRange segmentRange2 = Controller.SegmentRange.newBuilder().setSegmentId(segment2).setMinKey(0.4).setMaxKey(0.6).build();
        Controller.SegmentRange segmentRange3 = Controller.SegmentRange.newBuilder().setSegmentId(segment3).setMinKey(0.7).setMaxKey(1.0).build();

        List<Controller.SegmentRange> segmentsList = new ArrayList<Controller.SegmentRange>(3);
        segmentsList.add(segmentRange1);
        segmentsList.add(segmentRange2);
        segmentsList.add(segmentRange3);

        when(this.mockControllerService.getCurrentSegments(eq("scope"), eq("stream"), anyLong())).thenReturn(
                CompletableFuture.completedFuture(segmentsList));

        StreamSegments currentSegments = this.testController.getCurrentSegments("scope", "stream").join();
        assertEquals(3, currentSegments.getSegments().size());
        assertEquals(new Segment("scope", "stream", 1), currentSegments.getSegmentForKey(0.2));
        assertEquals(new Segment("scope", "stream", 3), currentSegments.getSegmentForKey(0.9));
    }

    @Test
    public void testListCompletedTransactions() {
        Map<UUID, TxnStatus> listResponse = new HashMap<>();
        listResponse.put(UUID.randomUUID(), TxnStatus.ABORTED);
        listResponse.put(UUID.randomUUID(), TxnStatus.COMMITTED);

        when(this.mockControllerService.listCompletedTxns(any(), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(listResponse));
        List<TransactionInfo> listTxns = this.testController.listCompletedTransactions(new StreamImpl("scope", "stream")).join();

        assertEquals(listTxns.size(), listResponse.keySet().size());
        assertEquals(listTxns.get(0).getStream().getStreamName(), "stream");
        assertEquals(listTxns.get(0).getStream().getScope(), "scope");
        assertEquals(listTxns.get(0).getTransactionStatus(), Transaction.Status.valueOf(listResponse.values().stream().collect(Collectors.toList()).get(0).name()));
        assertEquals(listTxns.get(0).getTransactionId(), listResponse.keySet().stream().collect(Collectors.toList()).get(0));
        Assert.assertTrue(listResponse.keySet().stream().collect(Collectors.toList()).contains(listTxns.get(0).getTransactionId()));
    }
}
