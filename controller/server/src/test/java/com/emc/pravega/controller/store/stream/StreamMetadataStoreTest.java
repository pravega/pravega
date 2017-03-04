/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


/**
 * Stream metadata test.
 */
public class StreamMetadataStoreTest {

    private final String scope = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
    private final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 3);
    private final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(scope).streamName(stream1).scalingPolicy(policy1).build();
    private final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(scope).streamName(stream2).scalingPolicy(policy2).build();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore store = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor);

    @Test
    public void testStreamMetadataStore() throws InterruptedException, ExecutionException {

        // region createStream
        store.createScope(scope).get();

        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.createStream(scope, stream2, configuration2, System.currentTimeMillis(), null, executor).get();

        assertEquals(stream1, store.getConfiguration(scope, stream1, null, executor).get().getStreamName());
        // endregion

        // region checkSegments
        List<Segment> segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(2, segments.size());

        SegmentFutures segmentFutures = store.getActiveSegments(scope, stream1, 10, null, executor).get();
        assertEquals(2, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segments = store.getActiveSegments(scope, stream2, null, executor).get();
        assertEquals(3, segments.size());

        segmentFutures = store.getActiveSegments(scope, stream2, 10, null, executor).get();
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        // endregion

        // region scaleSegments
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        store.scale(scope, stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20, null, executor).get();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());

        segmentFutures = store.getActiveSegments(scope, stream1, 30, null, executor).get();
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(scope, stream1, 10, null, executor).get();
        assertEquals(2, segmentFutures.getCurrent().size());
        assertEquals(2, segmentFutures.getFutures().size());

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        store.scale(scope, stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20, null, executor).get();

        segments = store.getActiveSegments(scope, stream1, null, executor).get();
        assertEquals(3, segments.size());

        segmentFutures = store.getActiveSegments(scope, stream2, 10, null, executor).get();
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(1, segmentFutures.getFutures().size());

        // endregion

        // region seal stream

        assertFalse(store.isSealed(scope, stream1, null, executor).get());
        assertNotEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());
        Boolean sealOperationStatus = store.setSealed(scope, stream1, null, executor).get();
        assertTrue(sealOperationStatus);
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        //Sealing an already seal stream should return success.
        Boolean sealOperationStatus1 = store.setSealed(scope, stream1, null, executor).get();
        assertTrue(sealOperationStatus1);
        assertTrue(store.isSealed(scope, stream1, null, executor).get());
        assertEquals(0, store.getActiveSegments(scope, stream1, null, executor).get().size());

        // seal a non-existent stream.
        try {
            store.setSealed(scope, "streamNonExistent", null, executor).get();
        } catch (Exception e) {
            assertEquals(DataNotFoundException.class, e.getCause().getCause().getClass());
        }
        // endregion
    }


    @Test
    public void listStreamsInScope() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor);
        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor);
        List<StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 2, streamInScope.size());
        assertEquals("List streams in scope", stream1, streamInScope.get(0).getStreamName());
        assertEquals("List streams in scope", stream2, streamInScope.get(1).getStreamName());

        // List streams in non-existent scope 'Scope1'
        try {
            store.listStreamsInScope("Scope1").get();
        } catch (StoreException se) {
            assertTrue("List streams in non-existent scope Scope1",
                    se.getType() == StoreException.Type.NODE_NOT_FOUND);
        }
    }

    @Test
    public void listScopes() throws Exception {
        // list scopes test
        List<String> list = store.listScopes().get();
        assertEquals("List Scopes size", 0, list.size());

        store.createScope("Scope1").get();
        store.createScope("Scope2").get();
        store.createScope("Scope3").get();
        store.createScope("Scope4").get();

        list = store.listScopes().get();
        assertEquals("List Scopes size", 4, list.size());

        store.deleteScope("Scope1").get();
        store.deleteScope("Scope2").get();
        list = store.listScopes().get();
        assertEquals("List Scopes size", 2, list.size());
    }
}
