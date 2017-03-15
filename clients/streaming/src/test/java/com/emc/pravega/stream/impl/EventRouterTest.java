/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EventRouterTest {

    private final String scope = "scope";
    private final String streamName = "streamName";

    @Test
    public void testUsesAllSegments() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        EventRouter router = new EventRouter(new StreamImpl(scope, streamName), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        router.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = router.getSegmentForEvent("" + i);
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }

    @Test
    public void testNullRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        EventRouter router = new EventRouter(new StreamImpl(scope, streamName), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        router.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 100; i++) {
            Segment segment = router.getSegmentForEvent(null);
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }

    @Test
    public void testSameRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        EventRouter router = new EventRouter(new StreamImpl(scope, streamName), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        router.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = router.getSegmentForEvent("Foo");
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

}
