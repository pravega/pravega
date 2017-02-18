/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.junit.Before;
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
    private final StreamConfiguration configuration1 = new StreamConfigurationImpl(scope, stream1, policy1);
    private final StreamConfiguration configuration2 = new StreamConfigurationImpl(scope, stream2, policy2);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore store =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor);

    @Before
    public void init() {
        PersistentStreamBase.setCreationState(State.ACTIVE);
    }

    @Test
    public void testStreamMetadataStore() throws InterruptedException, ExecutionException {

        // region createStream
        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor);
        store.createStream(scope, stream2, configuration2, System.currentTimeMillis(), null, executor);

        assertEquals(stream1, store.getConfiguration(scope, stream1, null, executor).get().getName());
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
        store.scale(scope, stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20, null, executor);

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
        store.scale(scope, stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20, null, executor);

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

}
