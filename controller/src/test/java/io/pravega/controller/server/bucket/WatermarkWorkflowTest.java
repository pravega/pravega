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
package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.impl.RevisionImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Synchronized;
import org.apache.curator.RetryPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WatermarkWorkflowTest {
    private static final RetryPolicy RETRY_POLICY = (r, e, s) -> false;
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(10000, 1000, RETRY_POLICY);

    StreamMetadataStore streamMetadataStore;
    BucketStore bucketStore;
    StreamMetadataTasks streamMetadataTasks;

    ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {

        executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

        streamMetadataStore = StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                                                                          GrpcAuthHelper.getDisabledAuthHelper(), PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 3,
                BucketStore.ServiceType.WatermarkingService, 3);
        bucketStore = StreamStoreFactory.createZKBucketStore(map, PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);

        streamMetadataTasks = new StreamMetadataTasks(streamMetadataStore, bucketStore, TaskStoreFactory.createInMemoryStore(executor),
                SegmentHelperMock.getSegmentHelperMock(), executor, "hostId", GrpcAuthHelper.getDisabledAuthHelper());

    }
    
    @After
    public void tearDown() throws Exception {
        streamMetadataStore.close();
        ExecutorServiceHelpers.shutdown(executor);

        streamMetadataStore.close();
    }
    
    @Test(timeout = 10000L)
    public void testWatermarkClient() {
        Stream stream = new StreamImpl("scope", "stream");
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);
        
        @Cleanup
        MockRevisionedStreamClient revisionedClient = new MockRevisionedStreamClient();
        doAnswer(x -> revisionedClient).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        @Cleanup
        PeriodicWatermarking.WatermarkClient client = new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
        // iteration 1 ==> null -> w1
        client.reinitialize();
        // There is no watermark in the stream. All values should be null and all writers active and participating.
        assertEquals(revisionedClient.getMark(), MockRevision.EMPTY);
        assertTrue(revisionedClient.watermarks.isEmpty());
        assertEquals(client.getPreviousWatermark(), Watermark.EMPTY);
        Map.Entry<String, WriterMark> entry0 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(0L, ImmutableMap.of()));
        Map.Entry<String, WriterMark> entry1 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(1L, ImmutableMap.of()));
        Map.Entry<String, WriterMark> entry2 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(2L, ImmutableMap.of()));
        Map.Entry<String, WriterMark> entry3 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(3L, ImmutableMap.of()));
        Map.Entry<String, WriterMark> entry4 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(4L, ImmutableMap.of()));
        Map.Entry<String, WriterMark> entry5 = new AbstractMap.SimpleEntry<>("writerId", new WriterMark(5L, ImmutableMap.of()));
        assertTrue(client.isWriterActive(entry0, 0L));
        assertTrue(client.isWriterParticipating(0L));
        Watermark first = new Watermark(1L, 2L, ImmutableMap.of());
        client.completeIteration(first);

        // iteration 2 : do not emit ==> w1 -> w1
        client.reinitialize();
        // There is one watermark. All writers should be active and writers greater than last watermark should be participating 
        assertEquals(revisionedClient.getMark(), MockRevision.EMPTY);
        assertEquals(revisionedClient.watermarks.size(), 1);
        assertEquals(client.getPreviousWatermark(), first);
        assertTrue(client.isWriterActive(entry2, 0L));
        assertFalse(client.isWriterActive(entry1, 0L));
        assertTrue(client.isWriterTracked(entry1.getKey()));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));
        // dont emit a watermark. Everything stays same as before. 
        client.completeIteration(null);
        
        // iteration 3 : emit ==> w1 -> w1 w2
        client.reinitialize();
        // There is one watermark. All writers should be active and writers greater than last watermark should be participating 
        assertEquals(revisionedClient.getMark(), MockRevision.EMPTY);
        assertEquals(revisionedClient.watermarks.size(), 1);
        assertEquals(client.getPreviousWatermark(), first);
        assertTrue(client.isWriterActive(entry2, 0L));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));
        // emit second watermark
        Watermark second = new Watermark(2L, 3L, ImmutableMap.of());
        client.completeIteration(second);

        // iteration 4: do not emit ==> w1 w2 -> w1 w2
        client.reinitialize();
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(0).getKey());
        assertEquals(2, revisionedClient.watermarks.size());
        assertEquals(client.getPreviousWatermark(), second);
        assertFalse(client.isWriterActive(entry2, 0L));
        assertTrue(client.isWriterTracked(entry2.getKey()));
        assertTrue(client.isWriterActive(entry3, 0L));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));

        assertTrue(client.isWriterActive(entry0, 1000L));
        assertTrue(client.isWriterTracked(entry0.getKey()));

        // dont emit a watermark but complete this iteration. 
        client.completeIteration(null);

        // iteration 6: emit ==> w1 w2 -> w1 w2 w3
        client.reinitialize();
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(0).getKey());
        assertEquals(2, revisionedClient.watermarks.size());
        assertEquals(client.getPreviousWatermark(), second);
        assertTrue(client.isWriterActive(entry3, 0L));
        assertFalse(client.isWriterTracked(entry3.getKey()));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));
        // emit third watermark
        Watermark third = new Watermark(3L, 4L, ImmutableMap.of());
        client.completeIteration(third);

        // iteration 7: do not emit ==> w1 w2 w3 -> w1 w2 w3
        client.reinitialize();
        // active writers should be ahead of first watermark. participating writers should be ahead of second watermark
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(3, revisionedClient.watermarks.size());
        assertEquals(client.getPreviousWatermark(), third);
        assertFalse(client.isWriterActive(entry3, 0L));
        assertTrue(client.isWriterActive(entry4, 0L));
        assertFalse(client.isWriterParticipating(3L));
        assertTrue(client.isWriterParticipating(4L));
        
        client.completeIteration(null);

        // iteration 8 : emit ==> w2 w3 -> w2 w3 w4
        client.reinitialize();
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        // window = w2 w3
        assertEquals(revisionedClient.watermarks.size(), 3);
        assertEquals(client.getPreviousWatermark(), third);
        assertFalse(client.isWriterActive(entry3, 0L));
        assertTrue(client.isWriterActive(entry4, 0L));
        assertFalse(client.isWriterParticipating(3L));
        assertTrue(client.isWriterParticipating(4L));

        // emit fourth watermark
        Watermark fourth = new Watermark(4L, 5L, ImmutableMap.of());
        client.completeIteration(fourth);

        // iteration 9: do not emit ==> w1 w2 w3 w4 -> w1 w2 w3 w4.. check writer timeout
        client.reinitialize();
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(2).getKey());
        assertEquals(revisionedClient.watermarks.size(), 4);
        assertEquals(client.getPreviousWatermark(), fourth);
        assertFalse(client.isWriterActive(entry3, 0L));
        assertTrue(client.isWriterTracked(entry4.getKey()));
        assertFalse(client.isWriterParticipating(4L));
        assertTrue(client.isWriterParticipating(5L));

        // verify that writer is active if we specify a higher timeout
        assertTrue(client.isWriterActive(entry1, 1000L));
        assertTrue(client.isWriterTracked(entry1.getKey()));
        // now that the writer is being tracked
        assertFalse(Futures.delayedTask(() -> client.isWriterActive(entry1, 1L), Duration.ofSeconds(1), executor).join());
        assertTrue(client.isWriterTracked(entry1.getKey()));

        // dont emit a watermark but complete this iteration. This should shrink the window again. 
        client.completeIteration(null);

        // iteration 10
        client.reinitialize();
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(2).getKey());
        assertEquals(revisionedClient.watermarks.size(), 4);
        assertEquals(client.getPreviousWatermark(), fourth);
        assertFalse(client.isWriterActive(entry4, 0L));
        assertTrue(client.isWriterActive(entry5, 0L));
        assertFalse(client.isWriterParticipating(4L));
        assertTrue(client.isWriterParticipating(5L));
    }
    
    @Test(timeout = 10000L)
    public void testWatermarkClientClose() {
        String scope = "scope1";
        String streamName = "stream1";
        StreamImpl stream = new StreamImpl(scope, streamName);
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);
        String markStreamName = NameUtils.getMarkStreamForStream(streamName);

        @Cleanup
        MockRevisionedStreamClient revisionedClient = new MockRevisionedStreamClient();
        doAnswer(x -> revisionedClient).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());
        doNothing().when(clientFactory).close();

        PeriodicWatermarking.WatermarkClient client = new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
        client.close();
        verify(clientFactory, never()).close();

        client = new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
        client.close();
        verify(clientFactory, never()).close();

        String s = "failing creation";
        doThrow(new RuntimeException(s)).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());
        AssertExtensions.assertThrows("constructor should throw", 
                () -> new PeriodicWatermarking.WatermarkClient(stream, clientFactory), e -> e instanceof RuntimeException && s.equals(e.getMessage()));
        
        @Cleanup
        PeriodicWatermarking periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, 
                sp -> clientFactory, executor, new RequestTracker(false));
        streamMetadataStore.createScope(scope, null, executor).join();
        streamMetadataStore.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(2)).timestampAggregationTimeout(10000L).build(),
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.createStream(scope, markStreamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.setState(scope, markStreamName, State.ACTIVE, null, executor).join();
        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();

        String writer1 = "writer1";
        Map<Long, Long> map1 = ImmutableMap.of(0L, 100L, 1L, 100L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 100L, map1, null, executor).join();

        // 2. run watermarking workflow. 
        periodicWatermarking.watermark(stream).join();
        assertTrue(periodicWatermarking.checkExistsInCache(scope));

        periodicWatermarking.evictFromCache(scope);
        // verify that the syncfactory was closed
        verify(clientFactory, times(1)).close();
    }

    @Test(timeout = 30000L)
    public void testWatermarkingWorkflow() {
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);

        ConcurrentHashMap<String, MockRevisionedStreamClient> revisionedStreamClientMap = new ConcurrentHashMap<>();

        doAnswer(x -> {
            String streamName = x.getArgument(0);
            return revisionedStreamClientMap.compute(streamName, (s, rsc) -> {
                if (rsc != null) {
                    return rsc;
                } else {
                    return new MockRevisionedStreamClient();
                }
            });
        }).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        @Cleanup
        PeriodicWatermarking periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, sp -> clientFactory, 
                executor, new RequestTracker(false));

        String streamName = "stream";
        String scope = "scope";
        streamMetadataStore.createScope(scope, null, executor).join();
        streamMetadataStore.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).timestampAggregationTimeout(10000L).build(), 
                System.currentTimeMillis(), null, executor).join();

        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();

        // set minimum number of segments to 1
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).timestampAggregationTimeout(10000L).build();
        streamMetadataStore.startUpdateConfiguration(scope, streamName, config, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamMetadataStore.getConfigurationRecord(scope, streamName, null, executor).join();
        streamMetadataStore.completeUpdateConfiguration(scope, streamName, configRecord, null, executor).join();

        // 2. note writer1, writer2, writer3 marks
        // writer 1 reports segments 0, 1. 
        // writer 2 reports segments 1, 2, 
        // writer 3 reports segment 0, 2
        String writer1 = "writer1";
        Map<Long, Long> map1 = ImmutableMap.of(0L, 100L, 1L, 200L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 100L, map1, null, executor).join();
        String writer2 = "writer2";
        Map<Long, Long> map2 = ImmutableMap.of(1L, 100L, 2L, 200L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer2, 101L, map2, null, executor).join();
        String writer3 = "writer3";
        Map<Long, Long> map3 = ImmutableMap.of(2L, 100L, 0L, 200L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer3, 102L, map3, null, executor).join();
        
        // 3. run watermarking workflow. 
        StreamImpl stream = new StreamImpl(scope, streamName);
        periodicWatermarking.watermark(stream).join();
        
        // verify that a watermark has been emitted. 
        // this should emit a watermark that contains all three segments with offsets = 200L
        // and timestamp = 100L
        MockRevisionedStreamClient revisionedClient = revisionedStreamClientMap.get(NameUtils.getMarkStreamForStream(streamName));
        assertEquals(revisionedClient.watermarks.size(), 1);
        Watermark watermark = revisionedClient.watermarks.get(0).getValue();
        assertEquals(watermark.getLowerTimeBound(), 100L);
        assertEquals(watermark.getStreamCut().size(), 3);
        assertEquals(getSegmentOffset(watermark, 0L), 200L);
        assertEquals(getSegmentOffset(watermark, 1L), 200L);
        assertEquals(getSegmentOffset(watermark, 2L), 200L);
        
        // send positions only on segment 1 and segment 2. nothing on segment 0.
        map1 = ImmutableMap.of(1L, 300L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 200L, map1, null, executor).join();
        map2 = ImmutableMap.of(1L, 100L, 2L, 300L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer2, 201L, map2, null, executor).join();
        map3 = ImmutableMap.of(2L, 300L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer3, 202L, map3, null, executor).join();

        // run watermark workflow. this will emit a watermark with time = 200L and streamcut = 0 -> 200L, 1 -> 300L, 2 -> 300L
        periodicWatermarking.watermark(stream).join();

        assertEquals(revisionedClient.watermarks.size(), 2);
        watermark = revisionedClient.watermarks.get(1).getValue();
        assertEquals(watermark.getLowerTimeBound(), 200L);
        assertEquals(watermark.getStreamCut().size(), 3);
        assertEquals(getSegmentOffset(watermark, 0L), 200L);
        assertEquals(getSegmentOffset(watermark, 1L), 300L);
        assertEquals(getSegmentOffset(watermark, 2L), 300L);

        // scale stream 0, 1, 2 -> 3, 4
        scaleStream(streamName, scope);

        // writer 1 reports segments 0, 1. 
        // writer 2 reports segments 1, 2 
        // writer 3 reports segment 3
        map1 = ImmutableMap.of(0L, 300L, 1L, 400L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 302L, map1, null, executor).join();
        map2 = ImmutableMap.of(1L, 100L, 2L, 400L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer2, 301L, map2, null, executor).join();
        long segment3 = NameUtils.computeSegmentId(3, 1);
        long segment4 = NameUtils.computeSegmentId(4, 1);
        map3 = ImmutableMap.of(segment3, 100L);
        // writer 3 has lowest reported time. 
        streamMetadataStore.noteWriterMark(scope, streamName, writer3, 300L, map3, null, executor).join();

        // run watermark workflow. this will emit a watermark with time = 300L and streamcut = 3 -> 100L, 4 -> 0L
        periodicWatermarking.watermark(stream).join();

        assertEquals(revisionedClient.watermarks.size(), 3);
        watermark = revisionedClient.watermarks.get(2).getValue();
        assertEquals(watermark.getLowerTimeBound(), 300L);
        assertEquals(watermark.getStreamCut().size(), 2);
        assertEquals(getSegmentOffset(watermark, segment3), 100L);
        assertEquals(getSegmentOffset(watermark, segment4), 0L);

        // report complete positions from writers. 
        // writer 1 reports 0, 1, 2
        // writer 2 reports 0, 1, 2
        // writer 3 doesnt report. 
        map1 = ImmutableMap.of(0L, 400L, 1L, 400L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 400L, map1, null, executor).join();
        map2 = ImmutableMap.of(1L, 100L, 2L, 400L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer2, 401L, map2, null, executor).join();

        // run watermark workflow. there shouldn't be a watermark emitted because writer 3 is active and has not reported a time. 
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 3);

        // even though writer3 is excluded from computation, its mark is still not removed because it is still active
        WriterMark writer3Mark = streamMetadataStore.getWriterMark(scope, streamName, writer3, null, executor).join();
        assertTrue(writer3Mark.isAlive());
        assertEquals(writer3Mark.getTimestamp(), 300L);

        // report shutdown of writer 3
        streamMetadataStore.shutdownWriter(scope, streamName, writer3, null, executor).join();
        writer3Mark = streamMetadataStore.getWriterMark(scope, streamName, writer3, null, executor).join();
        assertFalse(writer3Mark.isAlive());
        assertEquals(writer3Mark.getTimestamp(), 300L);

        // now a watermark should be generated. Time should be advanced. But watermark's stream cut is already ahead of writer's 
        // positions so stream cut should not advance.
        // Also writer 3 being inactive and shutdown, should be removed. 
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 4);
        watermark = revisionedClient.watermarks.get(3).getValue();
        assertEquals(watermark.getLowerTimeBound(), 400L);
        assertEquals(watermark.getStreamCut().size(), 2);
        assertEquals(getSegmentOffset(watermark, segment3), 100L);
        assertEquals(getSegmentOffset(watermark, segment4), 0L);

        AssertExtensions.assertFutureThrows("Writer 3 should have been removed from store",
                streamMetadataStore.getWriterMark(scope, streamName, writer3, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        
        // writer 1, 2 and 3 report marks. With writer 3 reporting mark on segment 4. Writer3 will get added again
        map1 = ImmutableMap.of(0L, 500L, 1L, 500L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 500L, map1, null, executor).join();
        map2 = ImmutableMap.of(1L, 100L, 2L, 500L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer2, 501L, map2, null, executor).join();
        map3 = ImmutableMap.of(segment4, 500L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer3, 502L, map3, null, executor).join();

        // run watermarking workflow. It should generate watermark that includes segments 3 -> 100L and 4 -> 500L with time 500L
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 5);
        watermark = revisionedClient.watermarks.get(4).getValue();
        assertEquals(watermark.getLowerTimeBound(), 500L);
        assertEquals(watermark.getStreamCut().size(), 2);
        assertEquals(getSegmentOffset(watermark, segment3), 100L);
        assertEquals(getSegmentOffset(watermark, segment4), 500L);
    }

    @Test(timeout = 30000L)
    public void testRevisionedClientThrowsNoSuchSegmentException() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        String markStreamName = NameUtils.getMarkStreamForStream(streamName);
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);

        ConcurrentHashMap<String, MockRevisionedStreamClient> revisionedStreamClientMap = new ConcurrentHashMap<>();

        doAnswer(x -> {
            String name = x.getArgument(0);
            return revisionedStreamClientMap.compute(name, (s, rsc) -> new MockRevisionedStreamClient(() -> 
                    streamMetadataStore.getActiveSegments(scope, name, null, executor).join().get(0).segmentId()));
        }).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        @Cleanup
        PeriodicWatermarking periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, 
                sp -> clientFactory, executor, new RequestTracker(false));

        streamMetadataStore.createScope(scope, null, executor).join();
        streamMetadataStore.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).timestampAggregationTimeout(10000L).build(), 
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.createStream(scope, markStreamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.setState(scope, markStreamName, State.ACTIVE, null, executor).join();
        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();
        
        // 1. note writer1 marks
        // writer 1 reports segments 0, 1. 
        String writer1 = "writer1";
        Map<Long, Long> map1 = ImmutableMap.of(0L, 100L, 1L, 100L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 100L, map1, null, executor).join();
        
        // 2. run watermarking workflow. 
        periodicWatermarking.watermark(stream).join();
        assertTrue(periodicWatermarking.checkExistsInCache(stream));

        // verify that a watermark has been emitted. 
        MockRevisionedStreamClient revisionedClient = revisionedStreamClientMap.get(markStreamName);
        assertEquals(revisionedClient.watermarks.size(), 1);
        Watermark watermark = revisionedClient.watermarks.get(0).getValue();
        assertEquals(watermark.getLowerTimeBound(), 100L);
        
        // delete and recreate stream and its mark stream
        streamMetadataStore.deleteStream(scope, markStreamName, null, executor).join();
        streamMetadataStore.deleteStream(scope, streamName, null, executor).join();
        streamMetadataStore.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).timestampAggregationTimeout(10000L).build(),
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();
        streamMetadataStore.createStream(scope, markStreamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), null, executor).join();
        streamMetadataStore.setState(scope, markStreamName, State.ACTIVE, null, executor).join();

        // 1. note writer1 marks
        // writer 1 reports segments 0, 1. 
        map1 = ImmutableMap.of(2L, 10L, 3L, 10L);
        streamMetadataStore.noteWriterMark(scope, streamName, writer1, 10L, map1, null, executor).join();

        // 2. run watermarking workflow. this should fail and revisioned stream client should be invalidated in the cache.
        periodicWatermarking.watermark(stream).join();
        assertFalse(periodicWatermarking.checkExistsInCache(stream));
        
        // 3. run watermarking workflow again.
        periodicWatermarking.watermark(stream).join();
        assertTrue(periodicWatermarking.checkExistsInCache(stream));

        // verify that a watermark has been emitted. 
        revisionedClient = revisionedStreamClientMap.get(markStreamName);
        assertEquals(revisionedClient.segment, 1L);
        assertEquals(revisionedClient.watermarks.size(), 1);
        watermark = revisionedClient.watermarks.get(0).getValue();
        assertEquals(watermark.getLowerTimeBound(), 10L);
    }

    @Test(timeout = 30000L)
    public void testWriterTimeout() {
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);

        ConcurrentHashMap<String, MockRevisionedStreamClient> revisionedStreamClientMap = new ConcurrentHashMap<>();

        doAnswer(x -> {
            String streamName = x.getArgument(0);
            return revisionedStreamClientMap.compute(streamName, (s, rsc) -> {
                if (rsc != null) {
                    return rsc;
                } else {
                    return new MockRevisionedStreamClient();
                }
            });
        }).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        StreamMetadataStore streamMetadataStoreSpied = spy(this.streamMetadataStore);
        BucketStore bucketStoreSpied = spy(this.bucketStore);
        @Cleanup
        PeriodicWatermarking periodicWatermarking = new PeriodicWatermarking(streamMetadataStoreSpied,
                bucketStoreSpied, sp -> clientFactory, executor, new RequestTracker(false));

        String streamName = "stream";
        String scope = "scope";
        streamMetadataStoreSpied.createScope(scope, null, executor).join();
        streamMetadataStoreSpied.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3))
                                                                               .timestampAggregationTimeout(3000L).build(),
                System.currentTimeMillis(), null, executor).join();

        streamMetadataStoreSpied.setState(scope, streamName, State.ACTIVE, null, executor).join();

        // 2. note writer1, writer2, writer3 marks
        // writer 1 reports segments 0, 1. 
        // writer 2 reports segments 1, 2, 
        // writer 3 reports segment 0, 2
        String writer1 = "writer1";
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer1, 102L,
                ImmutableMap.of(0L, 100L, 1L, 0L, 2L, 0L), null, executor).join();
        String writer2 = "writer2";
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer2, 101L,
                ImmutableMap.of(0L, 0L, 1L, 100L, 2L, 0L), null, executor).join();
        String writer3 = "writer3";
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer3, 100L,
                ImmutableMap.of(0L, 0L, 1L, 0L, 2L, 100L), null, executor).join();

        // 3. run watermarking workflow. 
        StreamImpl stream = new StreamImpl(scope, streamName);
        periodicWatermarking.watermark(stream).join();

        // verify that a watermark has been emitted. 
        MockRevisionedStreamClient revisionedClient = revisionedStreamClientMap.get(NameUtils.getMarkStreamForStream(streamName));
        assertEquals(revisionedClient.watermarks.size(), 1);

        // Don't report time from writer3
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer1, 200L,
                ImmutableMap.of(0L, 200L, 1L, 0L, 2L, 0L), null, executor).join();
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer2, 200L,
                ImmutableMap.of(0L, 0L, 1L, 200L, 2L, 0L), null, executor).join();

        // no new watermark should be emitted, writers should be tracked for inactivity
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 1);
        verify(streamMetadataStoreSpied, never()).removeWriter(anyString(), anyString(), anyString(), any(), any(), any());
        verify(bucketStoreSpied, never()).removeStreamFromBucketStore(any(), anyString(), anyString(), any());

        // call again. Still no new watermark should be emitted as writers have not timed out
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 1);
        verify(streamMetadataStoreSpied, never()).removeWriter(anyString(), anyString(), anyString(), any(), any(), any());
        verify(bucketStoreSpied, never()).removeStreamFromBucketStore(any(), anyString(), anyString(), any());
        
        // call watermark after a delay of 5 more seconds. The writer3 should timeout because it has a timeout of 3 seconds.
        Futures.delayedFuture(() -> periodicWatermarking.watermark(stream), 5000L, executor).join();

        verify(streamMetadataStoreSpied, times(1)).removeWriter(anyString(), anyString(), anyString(), any(), any(), any());
        verify(bucketStoreSpied, never()).removeStreamFromBucketStore(any(), anyString(), anyString(), any());
        
        // watermark should be emitted. without considering writer3 
        assertEquals(revisionedClient.watermarks.size(), 2);
        Watermark watermark = revisionedClient.watermarks.get(1).getValue();
        assertEquals(watermark.getLowerTimeBound(), 200L);
        assertEquals(watermark.getStreamCut().size(), 3);
        assertEquals(getSegmentOffset(watermark, 0L), 200L);
        assertEquals(getSegmentOffset(watermark, 1L), 200L);
        assertEquals(getSegmentOffset(watermark, 2L), 100L);

        // call watermark workflow again so that both writers are tracked for inactivity
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 2);
        verify(streamMetadataStoreSpied, times(1)).removeWriter(anyString(), anyString(), anyString(), any(), any(), any());
        verify(bucketStoreSpied, never()).removeStreamFromBucketStore(any(), anyString(), anyString(), any());

        // now introduce more delays and see all writers are removed and stream is discontinued from watermarking computation. 
        Futures.delayedFuture(() -> periodicWatermarking.watermark(stream), 5000L, executor).join();

        // verify that stream is discontinued from tracking for watermarking
        verify(streamMetadataStoreSpied, times(3)).removeWriter(anyString(), anyString(), anyString(), any(), any(), any());
        verify(bucketStoreSpied, times(1)).removeStreamFromBucketStore(any(), anyString(), anyString(), any());

        // call note time for writer3 and verify that watermark is emitted. 
        streamMetadataStoreSpied.noteWriterMark(scope, streamName, writer3, 300L,
                ImmutableMap.of(0L, 300L, 1L, 0L, 2L, 0L), null, executor).join();
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 3);
        watermark = revisionedClient.watermarks.get(2).getValue();
        assertEquals(watermark.getLowerTimeBound(), 300L);
        assertEquals(watermark.getStreamCut().size(), 3);
        assertEquals(getSegmentOffset(watermark, 0L), 300L);
        assertEquals(getSegmentOffset(watermark, 1L), 200L);
        assertEquals(getSegmentOffset(watermark, 2L), 100L);
    }
    
    private long getSegmentOffset(Watermark watermark, long segmentId) {
        return watermark.getStreamCut().entrySet().stream().filter(x -> x.getKey().getSegmentId() == segmentId)
                        .findFirst().get().getValue();
    }

    private void scaleStream(String streamName, String scope) {
        long scaleTs = System.currentTimeMillis();
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.5, 1.0);
        List<Long> sealedSegments = Lists.newArrayList(0L, 1L, 2L);
        VersionedMetadata<EpochTransitionRecord> response = streamMetadataStore.submitScale(scope, streamName, sealedSegments, Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        VersionedMetadata<State> state = streamMetadataStore.getVersionedState(scope, streamName, null, executor).join();
        state = streamMetadataStore.updateVersionedState(scope, streamName, State.SCALING, state, null, executor).join();
        response = streamMetadataStore.startScale(scope, streamName, false, response, state, null, executor).join();
        streamMetadataStore.scaleCreateNewEpochs(scope, streamName, response, null, executor).join();
        streamMetadataStore.scaleSegmentsSealed(scope, streamName, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).join();
        streamMetadataStore.completeScale(scope, streamName, response, null, executor).join();
        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();
    }

    static class MockRevisionedStreamClient implements RevisionedStreamClient<Watermark> {
        private final long segment;
        private final Supplier<Long> segmentSupplier;
        
        private final AtomicInteger revCounter = new AtomicInteger(0);
        private Revision mark;
        private final List<Map.Entry<Revision, Watermark>> watermarks = new ArrayList<>();

        MockRevisionedStreamClient() {
            this(() -> 0L);
        }

        MockRevisionedStreamClient(Supplier<Long> segmentSupplier) {
            this.segmentSupplier = segmentSupplier;
            this.segment = segmentSupplier.get();
        }

        @Override
        @Synchronized
        public Revision fetchOldestRevision() {
            checkValid();
            return watermarks.isEmpty() ? MockRevision.EMPTY : watermarks.get(0).getKey();
        }

        @Override
        @Synchronized
        public Revision fetchLatestRevision() {
            checkValid();
            return watermarks.isEmpty() ? MockRevision.EMPTY : watermarks.get(watermarks.size() - 1).getKey();
        }

        @Override
        @Synchronized
        public Iterator<Map.Entry<Revision, Watermark>> readFrom(Revision start) throws TruncatedDataException {
            checkValid();
            int index = start.equals(MockRevision.EMPTY) ? 0 : ((MockRevision) start).id + 1;
            return watermarks.stream().filter(x -> ((MockRevision) x.getKey()).id >= index).iterator();
        }

        @Override
        @Synchronized
        public Iterator<Map.Entry<Revision, Watermark>> readRange(Revision start, Revision end) throws TruncatedDataException {
            checkValid();
            int startIndex = start.equals(MockRevision.EMPTY) ? 0 : ((MockRevision) start).id + 1;
            int endIndex = end.equals(MockRevision.EMPTY) ? 0 : ((MockRevision) start).id;
            return watermarks.subList(startIndex, endIndex).iterator();
        }

        @Override
        @Synchronized
        public Revision writeConditionally(Revision latestRevision, Watermark value) {
            checkValid();
            Revision last = watermarks.isEmpty() ? MockRevision.EMPTY : watermarks.get(watermarks.size() - 1).getKey();
            boolean equal;
            Revision newRevision = null;
            equal = latestRevision.equals(last);

            if (equal) {
                newRevision = new MockRevision(revCounter.incrementAndGet());

                watermarks.add(new AbstractMap.SimpleEntry<>(newRevision, value));
            }
            return newRevision;
        }

        @Override
        public void writeUnconditionally(Watermark value) {
            checkValid();
        }

        @Override
        @Synchronized
        public Revision getMark() {
            checkValid();
            return mark;
        }

        @Override
        @Synchronized
        public boolean compareAndSetMark(Revision expected, Revision newLocation) {
            checkValid();
            boolean equal;
            if (expected == null) {
                equal = mark == null;
            } else {
                equal = expected.equals(mark);
            }

            if (equal) {
                mark = newLocation;
            }
            return equal;
        }

        @Override
        public void truncateToRevision(Revision revision) {
            checkValid();
        }

        @Override
        public void close() {

        }
        
        private void checkValid() {
            long segId  = segmentSupplier.get();
            if (segId != segment) {
                throw new NoSuchSegmentException("" + segment);
            }
        }
    }
    
    @EqualsAndHashCode
    static class MockRevision implements Revision {
        static final MockRevision EMPTY = new MockRevision(Integer.MIN_VALUE);
        
        private final int id;

        MockRevision(int id) {
            this.id = id;
        }

        @Override
        public RevisionImpl asImpl() {
            return null;
        }
        
        @Override
        public int compareTo(@NonNull Revision o) {
            return Integer.compare(id, ((MockRevision) o).id);
        }
    }
}
