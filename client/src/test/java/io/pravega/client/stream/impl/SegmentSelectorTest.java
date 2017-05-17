/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SegmentSelectorTest {

    private final String scope = "scope";
    private final String streamName = "streamName";

    @Test
    public void testUsesAllSegments() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), UUID.randomUUID(), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = selector.getSegmentForEvent("" + i);
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
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), UUID.randomUUID(), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 100; i++) {
            Segment segment = selector.getSegmentForEvent(null);
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
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), UUID.randomUUID(), controller, factory);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters();
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = selector.getSegmentForEvent("Foo");
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

}
