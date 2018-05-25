/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.CheckpointImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ReaderGroupConfigTest {
    private static final String SCOPE = "scope";

    @Test
    public void testValidConfigWithScopedStreamName() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream(SCOPE, "s1", getStreamCut(SCOPE, "s1"))
                .stream(SCOPE, "s2", getStreamCut(SCOPE, "s2"))
                .stream("s3", getStreamCut("defaultScope", "s3"))
                .stream("s4", StreamCut.UNBOUNDED, getStreamCut("defaultScope", "s4"))
                .defaultScope("defaultScope")
                .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut(SCOPE, "s1"), cfg.getStartingStreamCuts().get("scope/s1"));
        assertEquals(StreamCut.UNBOUNDED, cfg.getEndingStreamCuts().get("scope/s1"));
        assertEquals(getStreamCut(SCOPE, "s2"), cfg.getStartingStreamCuts().get("scope/s2"));
        assertEquals(StreamCut.UNBOUNDED, cfg.getEndingStreamCuts().get("scope/s2"));
        assertEquals(getStreamCut("defaultScope", "s3"), cfg.getStartingStreamCuts().get("defaultScope/s3"));
        assertEquals(StreamCut.UNBOUNDED, cfg.getStartingStreamCuts().get("defaultScope/s4"));
        assertEquals(getStreamCut("defaultScope", "s4"), cfg.getEndingStreamCuts().get("defaultScope/s4"));
    }

    @Test
    public void testStartFromCheckpoint() {
        Checkpoint checkpoint = Mockito.mock(Checkpoint.class);
        CheckpointImpl checkpointImpl = Mockito.mock(CheckpointImpl.class);
        when(checkpoint.asImpl()).thenReturn(checkpointImpl);
        when(checkpointImpl.getPositions()).thenReturn(ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1"))
                .put(Stream.of(SCOPE, "s2"), getStreamCut(SCOPE, "s2")).build());

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromCheckpoint(checkpoint)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut(SCOPE, "s1"), cfg.getStartingStreamCuts().get("scope/s1"));
        assertEquals(getStreamCut(SCOPE, "s2"), cfg.getStartingStreamCuts().get("scope/s2"));
    }

    @Test
    public void testStartFromStreamCuts() {
        Map<Stream, StreamCut> streamCuts = ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1"))
                .put(Stream.of("scope/s2"), getStreamCut(SCOPE, "s2")).build();

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromStreamCuts(streamCuts)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut(SCOPE, "s1"), cfg.getStartingStreamCuts().get("scope/s1"));
        assertEquals(getStreamCut(SCOPE, "s2"), cfg.getStartingStreamCuts().get("scope/s2"));
    }

    @Test
    public void testValidConfig() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("scope", "s1", getStreamCut(SCOPE, "s1"))
                                                 .stream(Stream.of(SCOPE, "s2"), getStreamCut(SCOPE, "s2"))
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut(SCOPE, "s1"), cfg.getStartingStreamCuts().get("scope/s1"));
        assertEquals(getStreamCut(SCOPE, "s2"), cfg.getStartingStreamCuts().get("scope/s2"));
    }

    @Test
    public void testValidConfigWithoutStartStreamCut() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("scope", "s1")
                                                 .stream("scope", "s2", getStreamCut(SCOPE, "s2"))
                                                 .stream("s3")
                                                 .stream("s4", getStreamCut("defaultScope", "s4"))
                                                 .defaultScope("defaultScope")
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(StreamCut.UNBOUNDED, cfg.getStartingStreamCuts().get("scope/s1"));
        assertEquals(getStreamCut(SCOPE, "s2"), cfg.getStartingStreamCuts().get("scope/s2"));
        assertEquals(StreamCut.UNBOUNDED, cfg.getStartingStreamCuts().get("defaultScope/s3"));
        assertEquals(getStreamCut("defaultScope", "s4"), cfg.getStartingStreamCuts().get("defaultScope/s4"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStreamNames() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyStreamName() {
        ReaderGroupConfig.builder()
                         .stream("")
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidStartStreamCut() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream("scope", "s1", getStreamCut(SCOPE, "s2"))
                         .stream("scope", "s2", getStreamCut(SCOPE, "s1"))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidStartStreamCutForStream() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s2"))
                         .stream("scope2", "s2", getStreamCut(SCOPE, "s1"))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingDefaultScope() {
        ReaderGroupConfig.builder()
                .stream(SCOPE, "s2", getStreamCut(SCOPE, "s2"))
                .stream("s3", getStreamCut("defaultScope", "s3"))
                .build();
    }

    @Test
    public void testValidStartAndEndStreamCuts() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1"), StreamCut.UNBOUNDED)
                         .stream(Stream.of(SCOPE, "s2"), StreamCut.UNBOUNDED, getStreamCut(SCOPE, "s2"))
                         .stream(Stream.of(SCOPE, "s3"))
                         .defaultScope("defaultScope")
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEndStreamCutForStream() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), StreamCut.UNBOUNDED, getStreamCut(SCOPE, "s2"))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStartAndEndStreamCuts() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1", 15L), getStreamCut(SCOPE, "s1", 10L))
                         .build();
    }

    @Test
    public void testValidStartAndEndStreamCutsWithSimilarSegments() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1", 10L), getStreamCut(SCOPE, "s1", 15L))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartAndEndStreamCutsWithOverlap() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1", 1, 2, 3),
                                 getStreamCut(SCOPE, "s1", 0, 7))
                         .build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartAndEndStreamCutsWithPartialOverlap() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut(SCOPE, "s1", 0, 7),
                                 getStreamCut(SCOPE, "s1", 5, 4, 6))
                         .build();
    }

    private StreamCut getStreamCut(String scope, String streamName) {
        return getStreamCut(scope, streamName, 10L);
    }

    private StreamCut getStreamCut(String scope, String streamName, int...segments) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segments).forEach(seg -> builder.put(new Segment(scope, streamName, seg), 10L));

        return new StreamCutImpl(Stream.of(scope, streamName), builder.build());
    }

    private StreamCut getStreamCut(String scope, String streamName, long offset) {
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(scope,
                streamName, 0), offset).build();
        return new StreamCutImpl(Stream.of(scope, streamName), positions);
    }
}
