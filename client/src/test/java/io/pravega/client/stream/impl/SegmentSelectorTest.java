/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;

import io.pravega.common.util.RetriesExhaustedException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static java.util.Collections.singletonList;
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
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.25);
        addNewSegment(segments, 1, 0.25, 0.5);
        addNewSegment(segments, 2, 0.5, 0.75);
        addNewSegment(segments, 3, 0.75, 1.0);
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

    private void addNewSegment(TreeMap<Double, SegmentWithRange> segments, long number, double low, double high) {
        segments.put(high, new SegmentWithRange(new Segment(scope, streamName, number), low, high));
    }

    @Test
    public void testNullRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.25);
        addNewSegment(segments, 1, 0.25, 0.5);
        addNewSegment(segments, 2, 0.5, 0.75);
        addNewSegment(segments, 3, 0.75, 1.0);
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
    public void testSameRoutingKey() {
        Controller controller = Mockito.mock(Controller.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.25);
        addNewSegment(segments, 1, 0.25, 0.5);
        addNewSegment(segments, 2, 0.5, 0.75);
        addNewSegment(segments, 3, 0.75, 1.0);
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
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.5);
        addNewSegment(segments, 1, 0.5, 1.0);
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
        assertFutureThrows("Writer Future", writerFuture, t -> t instanceof NoSuchSegmentException);
    }

    @Test
    public void testControllerNotReachable() {
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
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.5);
        addNewSegment(segments, 1, 0.5, 1.0);
        StreamSegments streamSegments = new StreamSegments(segments, "");

        when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(CompletableFuture.completedFuture(streamSegments));
        //trigger refresh.
        selector.refreshSegmentEventWriters(segmentSealedCallback);

        //simulate controller failure when controller.getSuccessors() is invoked.
        when(controller.getSuccessors(segment0))
                .thenAnswer(i -> {
                    CompletableFuture<StreamSegmentsWithPredecessors> result = new CompletableFuture<>();
                    // Controller client in case of RPC exceptions throws a StatusRuntimeException which is retried by the client.
                    // If the controller client is not able to reach the controller after all the retries a RetriesExhaustedException is
                    // thrown.
                    result.completeExceptionally(new RetriesExhaustedException(new StatusRuntimeException(Status.DATA_LOSS)));
                    return result;
                });

        assertEquals(Collections.emptyList(), selector.refreshSegmentEventWritersUponSealed(segment0, segmentSealedCallback));
        assertFutureThrows("Writer Future", writerFuture, t -> t instanceof ControllerFailureException);
    }

    @Test
    public void testSegmentRefreshOnSealed() {
        final Segment segment0 = new Segment(scope, streamName, 0);
        final Segment segment1 = new Segment(scope, streamName, computeSegmentId(1, 1));
        final Segment segment2 = new Segment(scope, streamName, computeSegmentId(2, 1));
        final CompletableFuture<Void> writerFuture = new CompletableFuture<>();
        final PendingEvent pendingEvent = PendingEvent.withHeader("0", ByteBuffer.wrap("e".getBytes()), writerFuture);

        TreeMap<Double, SegmentWithRange> segmentBeforeScale = new TreeMap<>();
        addNewSegment(segmentBeforeScale, 0, 0.0, 1.0);
        StreamSegments streamSegmentsBeforeScale = new StreamSegments(segmentBeforeScale, "");

        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(segment1, 0.0, 0.5), ImmutableList.of(segment0.getSegmentId()));
        newRange.put(new SegmentWithRange(segment2, 0.5, 1.0), ImmutableList.of(segment0.getSegmentId()));
        StreamSegmentsWithPredecessors segmentsWithPredecessors = new StreamSegmentsWithPredecessors(newRange, "");

        // Setup Mock.
        SegmentOutputStream s0Writer = Mockito.mock(SegmentOutputStream.class);
        SegmentOutputStream s1Writer = Mockito.mock(SegmentOutputStream.class);
        SegmentOutputStream s2Writer = Mockito.mock(SegmentOutputStream.class);
        SegmentOutputStreamFactory factory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        when(s0Writer.getUnackedEventsOnSeal()).thenReturn(ImmutableList.of(pendingEvent));
        when(factory.createOutputStreamForSegment(eq(segment0), ArgumentMatchers.<Consumer<Segment>>any(), any(EventWriterConfig.class), anyString()))
                .thenReturn(s0Writer);
        when(factory.createOutputStreamForSegment(eq(segment1), ArgumentMatchers.<Consumer<Segment>>any(), any(EventWriterConfig.class), anyString()))
                .thenReturn(s1Writer);
        when(factory.createOutputStreamForSegment(eq(segment2), ArgumentMatchers.<Consumer<Segment>>any(), any(EventWriterConfig.class), anyString()))
                .thenReturn(s2Writer);
        // get current segments returns segment 0
        when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(CompletableFuture.completedFuture(streamSegmentsBeforeScale));
        when(controller.getSuccessors(segment0))
                .thenAnswer(i -> {
                    CompletableFuture<StreamSegmentsWithPredecessors> result = new CompletableFuture<>();
                    result.complete(segmentsWithPredecessors);
                    return result;
                });

        SegmentSelector selector = new SegmentSelector(new StreamImpl(scope, streamName), controller, factory, config);
        //trigger refresh
        selector.refreshSegmentEventWriters(segmentSealedCallback);
        // only segment 0 writer is present.
        assertEquals(singletonList(s0Writer), selector.getWriters());

        // trigger a referesh of writers due to segment 0 being sealed.
        List<PendingEvent> pendingEvents = selector.refreshSegmentEventWritersUponSealed(segment0, segmentSealedCallback);
        // one pending event is returned by segment0
        assertEquals(singletonList(pendingEvent), pendingEvents);
        // the current number of writers is 3, it includes the writer to segment 0.
        List<SegmentOutputStream> writers = selector.getWriters();
        assertEquals(3, writers.size());
        assertTrue(writers.contains(s0Writer));
        assertTrue(writers.contains(s1Writer));
        assertTrue(writers.contains(s2Writer));
        // remove segment 0, this is done post resending the pending events.
        selector.removeSegmentWriter(segment0);
        assertFalse(selector.getWriters().contains(s0Writer));
    }

}
