/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.SynchronizerClientFactory;
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
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Synchronized;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class WatermarkWorkflowTest {
    TestingServer zkServer;
    CuratorFramework zkClient;

    StreamMetadataStore streamMetadataStore;
    BucketStore bucketStore;
    StreamMetadataTasks streamMetadataTasks;

    ScheduledExecutorService executor;
    
    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();

        executor = Executors.newScheduledThreadPool(10);

        streamMetadataStore = StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                AuthHelper.getDisabledAuthHelper(), zkClient, executor);
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 3,
                BucketStore.ServiceType.WatermarkingService, 3);
        bucketStore = StreamStoreFactory.createZKBucketStore(map, zkClient, executor);

        streamMetadataTasks = new StreamMetadataTasks(streamMetadataStore, bucketStore, TaskStoreFactory.createInMemoryStore(executor),
                SegmentHelperMock.getSegmentHelperMock(), executor, "hostId", AuthHelper.getDisabledAuthHelper(), new RequestTracker(false));

    }
    
    @After
    public void tearDown() throws Exception {
        streamMetadataStore.close();
        ExecutorServiceHelpers.shutdown(executor);

        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }
    
    @Test(timeout = 10000L)
    public void testWatermarkClient() {
        Stream stream = new StreamImpl("scope", "stream");
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);
        
        MockRevisionedStreamClient revisionedClient = new MockRevisionedStreamClient();
        doAnswer(x -> revisionedClient).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        PeriodicWatermarking.WatermarkClient client = new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
        
        // iteration 1
        client.reinitialize();
        // There is no watermark in the stream. All values should be null and all writers active and participating.
        assertNull(revisionedClient.getMark());
        assertTrue(revisionedClient.watermarks.isEmpty());
        assertEquals(client.getPreviousWatermark(), Watermark.EMPTY);
        assertTrue(client.isWriterActive(0L));
        assertTrue(client.isWriterParticipating(0L));
        Watermark first = new Watermark(1L, 2L, ImmutableMap.of());
        client.completeIteration(first);

        // iteration 2
        client.reinitialize();
        // There is one watermark. All writers should be active and writers greater than last watermark should be participating 
        assertNull(revisionedClient.getMark());
        assertEquals(revisionedClient.watermarks.size(), 1);
        assertEquals(client.getPreviousWatermark(), first);
        assertTrue(client.isWriterActive(0L));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));
        
        // emit second watermark
        Watermark second = new Watermark(2L, 3L, ImmutableMap.of());
        client.completeIteration(second);

        // iteration 3.. do not emit
        client.reinitialize();
        // There are two watermarks. Window size is also 2. So all writers should be active. 
        // All writers after second watermark should be participating. 
        // Mark should be null. 
        assertNull(revisionedClient.getMark());
        assertEquals(2, revisionedClient.watermarks.size());
        assertEquals(client.getPreviousWatermark(), second);
        assertTrue(client.isWriterActive(0L));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));

        // dont emit a watermark but complete this iteration. 
        // This should proceed to shrink the active window.
        client.completeIteration(null);

        // iteration 4.. do not emit
        client.reinitialize();
        // Mark should have been set to first watermark's revision
        // writer's with time before first watermark will be inactive. Writers after second watermark should be participating.  
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(0).getKey());
        assertEquals(revisionedClient.watermarks.size(), 2);
        assertEquals(client.getPreviousWatermark(), second);
        assertFalse(client.isWriterActive(1L));
        assertTrue(client.isWriterActive(2L));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));

        // dont emit a watermark but complete this iteration. This should shrink the window again. 
        client.completeIteration(null);

        // iteration 5
        client.reinitialize();
        // mark should be set to revision of second watermark.
        // active writers should be ahead of second watermark. participating writers should be ahead of second watermark
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 2);
        assertEquals(client.getPreviousWatermark(), second);
        assertFalse(client.isWriterActive(2L));
        assertTrue(client.isWriterActive(3L));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));
        // emit third watermark
        Watermark third = new Watermark(3L, 4L, ImmutableMap.of());
        client.completeIteration(third);

        // iteration 6
        client.reinitialize();
        // mark should still be set to revision of second watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 3);
        assertEquals(client.getPreviousWatermark(), third);
        assertFalse(client.isWriterActive(2L));
        assertTrue(client.isWriterActive(3L));
        assertFalse(client.isWriterParticipating(3L));
        assertTrue(client.isWriterParticipating(4L));
        
        // emit fourth watermark
        Watermark fourth = new Watermark(4L, 5L, ImmutableMap.of());
        client.completeIteration(fourth);

        // iteration 7
        client.reinitialize();
        // mark should still be set to revision of second watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 4);
        assertEquals(client.getPreviousWatermark(), fourth);
        assertFalse(client.isWriterActive(2L));
        assertTrue(client.isWriterActive(3L));
        assertFalse(client.isWriterParticipating(4L));
        assertTrue(client.isWriterParticipating(5L));

        // emit fifth watermark
        Watermark fifth = new Watermark(5L, 6L, ImmutableMap.of());
        client.completeIteration(fifth);

        // iteration 8
        client.reinitialize();
        // mark should progress to third watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(2).getKey());
        assertEquals(revisionedClient.watermarks.size(), 5);
        assertEquals(client.getPreviousWatermark(), fifth);
        assertFalse(client.isWriterActive(3L));
        assertTrue(client.isWriterActive(4L));
        assertFalse(client.isWriterParticipating(5L));
        assertTrue(client.isWriterParticipating(6L));
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

        ConcurrentHashMap<Stream, PeriodicWatermarking.WatermarkClient> watermarkClientMap = new ConcurrentHashMap<>();
        
        Function<Stream, PeriodicWatermarking.WatermarkClient> supplier = stream -> {
            return watermarkClientMap.compute(stream, (s, wc) -> {
               if (wc != null) {
                   return wc;
               } else {
                   return new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
               }
            });
        };

        PeriodicWatermarking periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, streamMetadataTasks, supplier, executor);

        String streamName = "stream";
        String scope = "scope";
        streamMetadataStore.createScope(scope).join();
        streamMetadataStore.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build(), 
                System.currentTimeMillis(), null, executor).join();

        streamMetadataStore.setState(scope, streamName, State.ACTIVE, null, executor).join();
        
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
        MockRevisionedStreamClient revisionedClient = revisionedStreamClientMap.get(StreamSegmentNameUtils.getMarkSegmentForStream(streamName));
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
        long segment3 = StreamSegmentNameUtils.computeSegmentId(3, 1);
        long segment4 = StreamSegmentNameUtils.computeSegmentId(4, 1);
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

        // run watermark workflow. there shouldn't be a watermark emitted until writer 3 times out and is excluded 
        // from computation. That will happen for two iterations because our window size is 2. Third iteration will
        // exclude writer 3 from watermark computation and remove it. 
        periodicWatermarking.watermark(stream).join();
        assertEquals(revisionedClient.watermarks.size(), 3);
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
        private final AtomicInteger revCounter = new AtomicInteger(0);
        private Revision mark;
        private final List<Map.Entry<Revision, Watermark>> watermarks = new ArrayList<>();
        
        @Override
        @Synchronized
        public Revision fetchOldestRevision() {
            return watermarks.isEmpty() ? MockRevision.EMPTY : watermarks.get(0).getKey();
        }

        @Override
        @Synchronized
        public Revision fetchLatestRevision() {
            return watermarks.isEmpty() ? MockRevision.EMPTY : watermarks.get(watermarks.size() - 1).getKey();
        }

        @Override
        @Synchronized
        public Iterator<Map.Entry<Revision, Watermark>> readFrom(Revision start) throws TruncatedDataException {
            int index = start.equals(MockRevision.EMPTY) ? 0 : ((MockRevision) start).id;
            return watermarks.stream().filter(x -> ((MockRevision) x.getKey()).id >= index).iterator();
        }

        @Override
        @Synchronized
        public Revision writeConditionally(Revision latestRevision, Watermark value) {
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
            
        }

        @Override
        @Synchronized
        public Revision getMark() {
            return mark;
        }

        @Override
        @Synchronized
        public boolean compareAndSetMark(Revision expected, Revision newLocation) {
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

        }

        @Override
        public void close() {

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
