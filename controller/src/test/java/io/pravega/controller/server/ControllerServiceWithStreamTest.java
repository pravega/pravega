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
package io.pravega.controller.server;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for ControllerService With ZK Stream Store
 */
@Slf4j
public abstract class ControllerServiceWithStreamTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private static final String STREAM1 = "stream1";

    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected CuratorFramework zkClient;

    private ControllerService consumer;

    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;

    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private ConnectionFactory connectionFactory;
    private StreamMetadataStore streamStore;
    private RequestTracker requestTracker = new RequestTracker(true);

    @Mock
    private KVTableMetadataStore kvtStore;
    @Mock
    private TableMetadataTasks kvtMetadataTasks;

    @Before
    public void setup() {
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            log.error("Error starting ZK server", e);
        }
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        streamStore = spy(getStore());
        kvtStore = spy(getKVTStore());
        BucketStore bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                  .controllerURI(URI.create("tcp://localhost"))
                                                                  .build());
        GrpcAuthHelper disabledAuthHelper = GrpcAuthHelper.getDisabledAuthHelper();
        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        StreamMetrics.initialize();
        TransactionMetrics.initialize();
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executor, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", disabledAuthHelper, helperMock);
        kvtMetadataTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), helperMock));
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, 
                "host", disabledAuthHelper);
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtMetadataTasks, executor),
                executor);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executor));
        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, 
                streamTransactionMetadataTasks, segmentHelperMock, executor, null, requestTracker);
    }

    abstract StreamMetadataStore getStore();

    abstract KVTableMetadataStore getKVTStore();

    @After
    public void teardown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        kvtStore.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 5000)
    public void createStreamTest() throws Exception {
        String stream = "create";
        String createStream = "_readerGroupStream";
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                                                                      .scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE, 0L).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // create internal stream with invalid name
        Controller.CreateStreamStatus streamStatus1 = consumer.createInternalStream(SCOPE, "$InvalidStream", configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.INVALID_STREAM_NAME, streamStatus1.getStatus());
        
        // create stream
        Controller.CreateStreamStatus streamStatus = consumer.createStream(SCOPE, stream, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        // create stream starting with underscore
        Controller.CreateStreamStatus createStreamStatus = consumer.createStream(SCOPE, createStream, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, createStreamStatus.getStatus());

        // there will be two invocations per call because we also create internal mark stream
        verify(streamStore, times(4)).createStream(anyString(), anyString(), any(), anyLong(), any(), any());
        
        streamStatus = consumer.createInternalStream(SCOPE, stream, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.STREAM_EXISTS, streamStatus.getStatus());

        // verify that create stream is not called again
        verify(streamStore, times(4)).createStream(anyString(), anyString(), any(), anyLong(), any(), any());
    }

    @Test(timeout = 5000)
    public void getSegmentsImmediatelyFollowingTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                                                                      .scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create stream and scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE, 0L).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());
        Controller.CreateStreamStatus streamStatus = consumer.createStream(SCOPE, STREAM1, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        List<Controller.SegmentRange> currentSegments = consumer.getCurrentSegments(SCOPE, STREAM1, 0L).get();
        assertEquals(2, currentSegments.size());

        //scale segment 1 which has key range from 0.5 to 1.0 at time: start+20
        Map<Double, Double> keyRanges = new HashMap<>(2);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);

        assertFalse(consumer.checkScale(SCOPE, STREAM1, 0, 0L).get().getStatus().equals(
                Controller.ScaleStatusResponse.ScaleStatus.SUCCESS));
        Controller.ScaleResponse scaleStatus = consumer.scale(SCOPE, STREAM1, Arrays.asList(1L), keyRanges, start + 20, 0L)
                                                       .get();
        assertEquals(Controller.ScaleResponse.ScaleStreamStatus.STARTED, scaleStatus.getStatus());
        AtomicBoolean done = new AtomicBoolean(false);
        Futures.loop(() -> !done.get(), () -> consumer.checkScale(SCOPE, STREAM1, scaleStatus.getEpoch(), 0L)
                                                      .thenAccept(x -> done.set(x.getStatus().equals(
                                                              Controller.ScaleStatusResponse.ScaleStatus.SUCCESS))), executor).get();
        //After scale the current number of segments is 3;
        List<Controller.SegmentRange> currentSegmentsAfterScale = consumer.getCurrentSegments(SCOPE, STREAM1, 0L).get();
        assertEquals(3, currentSegmentsAfterScale.size());

        Map<Controller.SegmentRange, List<Long>> successorsOfSeg1 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM1, 1), 0L).get();
        assertEquals(2, successorsOfSeg1.size()); //two segments follow segment 1

        Map<Controller.SegmentRange, List<Long>> successorsOfSeg0 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM1, 0), 0L).get();
        assertEquals(0, successorsOfSeg0.size()); //no segments follow segment 0
    }

    @Test(timeout = 50000)
    public void streamCutTests() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                                                                      .scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create stream and scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE, 0L).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());
        Controller.CreateStreamStatus streamStatus = consumer.createStream(SCOPE, STREAM, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        List<Long> segments = new ArrayList<>();
        segments.add(0L);
        segments.add(1L);

        // perform following scales
        // 0, 1
        // 0, 2, 3
        // 4, 2, 3
        // 5, 6
        Map<Double, Double> keyRanges = new HashMap<>(2);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);
        scale(System.currentTimeMillis(), Arrays.asList(1L), keyRanges);
        // new segments after scale = {(0.0), (2.1), (3.1)}
        long two = NameUtils.computeSegmentId(2, 1);
        long three = NameUtils.computeSegmentId(3, 1);
        segments.add(two);
        segments.add(three);

        keyRanges = new HashMap<>(1);
        keyRanges.put(0.0, 0.5);
        scale(System.currentTimeMillis(), Arrays.asList(0L), keyRanges);
        // new segments after scale = {(4.2), (2.1), (3.1)}
        long four = NameUtils.computeSegmentId(4, 2);
        segments.add(four);

        keyRanges = new HashMap<>(1);
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        scale(System.currentTimeMillis(), Arrays.asList(two, three, four), keyRanges);
        // new segments after scale = {(5.3), (6.3)}
        long five = NameUtils.computeSegmentId(5, 3);
        long six = NameUtils.computeSegmentId(6, 3);
        segments.add(five);
        segments.add(six);

        // create stream cuts:
        Map<Long, Long> streamCut01 = new HashMap<>();
        streamCut01.put(0L, 0L);
        streamCut01.put(1L, 0L);

        Map<Long, Long> streamCut023 = new HashMap<>();
        streamCut023.put(0L, 0L);
        streamCut023.put(two, 0L);
        streamCut023.put(three, 0L);

        Map<Long, Long> streamCut423 = new HashMap<>();
        streamCut423.put(four, 0L);
        streamCut423.put(two, 0L);
        streamCut423.put(three, 0L);

        Map<Long, Long> streamCut56 = new HashMap<>();
        streamCut56.put(five, 0L);
        streamCut56.put(six, 0L);

        Map<Long, Long> streamCut41 = new HashMap<>();
        streamCut41.put(four, 0L);
        streamCut41.put(1L, 0L);

        Map<Long, Long> streamCut06 = new HashMap<>();
        streamCut06.put(0L, 0L);
        streamCut06.put(six, 0L);

        Map<Long, Long> invalidStreamCutOverlap = new HashMap<>();
        invalidStreamCutOverlap.put(0L, 0L);
        invalidStreamCutOverlap.put(five, 0L);

        Map<Long, Long> invalidStreamCutMissingRange = new HashMap<>();
        invalidStreamCutMissingRange.put(0L, 0L);

        // send empty stream cuts for `to`
        testEmptyFrom(streamCut01, streamCut023, streamCut423, streamCut56, streamCut41, streamCut06, segments);

        // send empty stream cuts for `from`
        testEmptyTo(streamCut01, streamCut023, streamCut423, streamCut56, streamCut41, streamCut06, segments);

        // send `from` before `to`
        testToBeforeFrom(streamCut01, streamCut023, streamCut56);

        // send valid `from` and `to`
        testValidRanges(streamCut01, streamCut023, streamCut423, streamCut56, streamCut41, streamCut06, segments);
    }

    @Test(timeout = 5000)
    public void createReaderGroupTest() throws Exception {
        String stream = "stream1";
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE, 0L).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        // create stream
        Controller.CreateStreamStatus streamStatus = consumer.createStream(SCOPE, stream, configuration1, start, 0L).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        final Segment seg0 = new Segment(SCOPE, "stream1", 0L);
        final Segment seg1 = new Segment(SCOPE, "stream1", 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of(SCOPE, "stream1"),
                new StreamCutImpl(Stream.of(SCOPE, "stream1"), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(Stream.of(SCOPE, "stream1"),
                new StreamCutImpl(Stream.of(SCOPE, "stream1"), endStreamCut));
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC)
                .build();
        Controller.CreateReaderGroupResponse rgStatus =  consumer.createReaderGroup(SCOPE, "rg1",
                                                        rgConfig, System.currentTimeMillis(), 0L).get();
        assertEquals(Controller.CreateReaderGroupResponse.Status.SUCCESS, rgStatus.getStatus());

        // there should be 1 invocation
        verify(streamStore, times(1)).createReaderGroup(anyString(), anyString(), any(), anyLong(), any(), any());

        rgStatus = consumer.createReaderGroup(SCOPE, "rg1", rgConfig, System.currentTimeMillis(), 0L).get();
        assertEquals(Controller.CreateReaderGroupResponse.Status.SUCCESS, rgStatus.getStatus());

        // verify that create readergroup is not called again
        verify(streamStore, times(1)).createReaderGroup(anyString(), anyString(), any(), anyLong(), any(), any());
    }

    private void testValidRanges(Map<Long, Long> streamCut01, Map<Long, Long> streamCut023, Map<Long, Long> streamCut423,
                                 Map<Long, Long> streamCut56, Map<Long, Long> streamCut41, Map<Long, Long> streamCut06,
                                 List<Long> segmentIds) throws ExecutionException, InterruptedException {
        Controller.StreamCutRange.Builder rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        Controller.StreamCutRange streamCutRange;
        streamCutRange = rangeBuilder.putAllFrom(streamCut01).putAllTo(streamCut023).build();
        List<StreamSegmentRecord> segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(4, segments.size());
        assertTrue(segments.stream().anyMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut01).putAllTo(streamCut423).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut01).putAllTo(streamCut06).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut41).putAllTo(streamCut56).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(6, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(1) || x.segmentId() == segmentIds.get(2) ||
                x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4) || x.segmentId() == segmentIds.get(5) ||
                x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut01).putAllTo(streamCut06).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(6)));
    }

    private void testToBeforeFrom(Map<Long, Long> streamCut01, Map<Long, Long> streamCut023, Map<Long, Long> streamCut56) {
        Controller.StreamCutRange.Builder rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        Controller.StreamCutRange streamCutRange;
        streamCutRange = rangeBuilder.putAllFrom(streamCut023).putAllTo(streamCut01).build();
        AssertExtensions.assertFutureThrows("to before from", consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L),
                e -> e instanceof IllegalArgumentException);

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut56).putAllTo(streamCut023).build();
        AssertExtensions.assertFutureThrows("to before from", consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L), 
                e -> e instanceof IllegalArgumentException);
    }

    private void testEmptyTo(Map<Long, Long> streamCut01, Map<Long, Long> streamCut023, Map<Long, Long> streamCut423,
                             Map<Long, Long> streamCut56, Map<Long, Long> streamCut41, Map<Long, Long> streamCut06,
                             List<Long> segmentIds) throws ExecutionException, InterruptedException {
        Controller.StreamCutRange.Builder rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        Controller.StreamCutRange streamCutRange;
        streamCutRange = rangeBuilder.putAllFrom(streamCut01).putAllTo(Collections.emptyMap()).build();
        List<StreamSegmentRecord> segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(7, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut023).putAllTo(Collections.emptyMap()).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(6, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut423).putAllTo(Collections.emptyMap()).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || 
                x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut56).putAllTo(Collections.emptyMap()).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(2, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut41).putAllTo(Collections.emptyMap()).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(6, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(streamCut06).putAllTo(Collections.emptyMap()).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(4, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));
    }

    private void testEmptyFrom(Map<Long, Long> streamCut01, Map<Long, Long> streamCut023, Map<Long, Long> streamCut423,
                               Map<Long, Long> streamCut56, Map<Long, Long> streamCut41, Map<Long, Long> streamCut06,
                               List<Long> segmentIds) throws InterruptedException, ExecutionException {
        Controller.StreamCutRange.Builder rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        Controller.StreamCutRange streamCutRange;
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut01).build();
        List<StreamSegmentRecord> segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(2, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut023).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(4, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut423).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut56).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(7, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(4) ||
                x.segmentId() == segmentIds.get(5) || x.segmentId() == segmentIds.get(6)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut41).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(3, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(4)));

        rangeBuilder = Controller.StreamCutRange.newBuilder().setStreamInfo(
                Controller.StreamInfo.newBuilder().setScope(SCOPE).setStream(STREAM).build());
        streamCutRange = rangeBuilder.putAllFrom(Collections.emptyMap()).putAllTo(streamCut06).build();
        segments = consumer.getSegmentsBetweenStreamCuts(streamCutRange, 0L).get();
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.segmentId() == segmentIds.get(0) || x.segmentId() == segmentIds.get(1) ||
                x.segmentId() == segmentIds.get(2) || x.segmentId() == segmentIds.get(3) || x.segmentId() == segmentIds.get(6)));
    }

    private void scale(long start, List<Long> segmentsToSeal, Map<Double, Double> keyRanges) throws InterruptedException, ExecutionException {
        Controller.ScaleResponse scaleStatus = consumer.scale(SCOPE, STREAM, segmentsToSeal, keyRanges, start, 0L)
                                                       .get();
        AtomicBoolean done = new AtomicBoolean(false);
        Futures.loop(() -> !done.get(), () -> Futures.delayedFuture(() -> consumer.checkScale(SCOPE, STREAM, scaleStatus.getEpoch(), 0L),
                1000, executor).thenAccept(x -> done.set(x.getStatus().equals(
                        Controller.ScaleStatusResponse.ScaleStatus.SUCCESS))), executor).get();
    }
}
