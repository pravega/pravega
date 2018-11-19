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

import com.google.common.collect.ImmutableList;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class SegmentSelectorTest {

    private final String scope = "scope";
    private final String streamName = "streamName";
    private final EventWriterConfig config = EventWriterConfig.builder().build();
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    @Test
    public void testUsesAllSegments() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters(segmentSealedCallback);
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = selector.getSegmentForEvent("" + i);
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }

    @Test
    public void testNullRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters(segmentSealedCallback);
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 100; i++) {
            Segment segment = selector.getSegmentForEvent(null);
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }

    @Test
    public void testSegmentIdAsRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, Segment> segments = new TreeMap<>();

        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters(segmentSealedCallback);
        for (int i = 0; i < 4; i++) {
            Segment segment = selector.getSegmentForId(i);
            assertNotNull(segment);
            assertEquals("scope/streamName/" + i + ".#epoch.0", segment.toString());
        }
        assertNull(selector.getSegmentForId(-1));
        assertNull(selector.getSegmentForId(4));
    }



    @Test
    public void testSameRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(CompletableFuture.completedFuture(streamSegments));
        selector.refreshSegmentEventWriters(segmentSealedCallback);
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = selector.getSegmentForEvent("Foo");
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

    @Test
    public void testStreamDeletion() {
        final Segment segment0 = new Segment(scope, streamName, 0);
        final Segment segment1 = new Segment(scope, streamName, 1);
        final CompletableFuture<Void> writerFuture = new CompletableFuture<>();

        // Setup Mock.
        SegmentOutputStream s0Writer = Mockito.mock(SegmentOutputStream.class);
        SegmentOutputStream s1Writer = Mockito.mock(SegmentOutputStream.class);
        when(s0Writer.getUnackedEventsOnSeal())
                .thenReturn(ImmutableList.of(PendingEvent.withHeader("0", ByteBuffer.wrap("e".getBytes()), writerFuture)));

        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        when(factory.createOutputStreamForSegment(eq(segment0), ArgumentMatchers.<Consumer<Segment>>any(), any(EventWriterConfig.class), anyString()))
                .thenReturn(s0Writer);
        when(factory.createOutputStreamForSegment(eq(segment1), ArgumentMatchers.<Consumer<Segment>>any(), any(EventWriterConfig.class), anyString()))
                .thenReturn(s1Writer);

        Controller controller = Mockito.mock(Controller.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.5, segment0);
        segments.put(1.0, segment1);
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(CompletableFuture.completedFuture(streamSegments));
        //trigger refresh.
        selector.refreshSegmentEventWriters(segmentSealedCallback);

        //simulate stream deletion where controller.getSuccessors() is completed exceptionally.
        when(controller.getSuccessors(segment0))
                .thenAnswer(i -> {
                    CompletableFuture<StreamSegmentsWithPredecessors> result = new CompletableFuture<>();
                    // Controller throws io.pravega.controller.store.stream.StoreException$DataNotFoundException which is type RuntimeException.
                    // Using RunTimeException here as the controller exception is not visible.
                    result.completeExceptionally(new RuntimeException());
                    return result;
                });

        assertEquals(Collections.emptyList(), selector.refreshSegmentEventWritersUponSealed(segment0, segmentSealedCallback));
        assertThrows("Writer Future", writerFuture, t -> t instanceof NoSuchSegmentException);
    }

}
