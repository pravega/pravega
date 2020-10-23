/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ReaderGroupConfigTest {
    private static final String SCOPE = "scope";

    @Test
    public void testValidConfigWithScopedStreamName() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .stream("scope/s2", getStreamCut("s2"))
                .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
    }

    @Test
    public void testStartFromCheckpoint() {
        Checkpoint checkpoint = Mockito.mock(Checkpoint.class);
        CheckpointImpl checkpointImpl = Mockito.mock(CheckpointImpl.class);
        when(checkpoint.asImpl()).thenReturn(checkpointImpl);
        when(checkpointImpl.getPositions()).thenReturn(ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStreamCut("s1"))
                .put(Stream.of(SCOPE, "s2"), getStreamCut("s2")).build());

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromCheckpoint(checkpoint)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
    }

    @Test
    public void testStartFromStreamCuts() {
        Map<Stream, StreamCut> streamCuts = ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStreamCut("s1"))
                .put(Stream.of("scope/s2"), getStreamCut("s2")).build();

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromStreamCuts(streamCuts)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
    }

    @Test
    public void testIsSubscriber() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .stream(Stream.of(SCOPE, "s2"), getStreamCut("s2"))
                .isSubscribedForRetention()
                .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
        assertTrue(cfg.isSubscriber());
        assertFalse(cfg.isAutoTruncateAtLastCheckpoint());
    }

    @Test
    public void testAutoTruncateAtLastCheckpoint() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .stream(Stream.of(SCOPE, "s2"), getStreamCut("s2"))
                .isSubscribedForRetention()
                .autoPublishCheckpoint()
                .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
        assertTrue(cfg.isSubscriber());
        assertTrue(cfg.isAutoTruncateAtLastCheckpoint());
    }

    @Test
    public void testValidConfig() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("scope/s1", getStreamCut("s1"))
                                                 .stream(Stream.of(SCOPE, "s2"), getStreamCut("s2"))
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStreamCut("s1"), cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
        assertFalse(cfg.isSubscriber());
        assertFalse(cfg.isAutoTruncateAtLastCheckpoint());
    }

    @Test
    public void testValidConfigWithoutStartStreamCut() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("scope/s1")
                                                 .stream("scope/s2", getStreamCut("s2"))
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(StreamCut.UNBOUNDED, cfg.getStartingStreamCuts().get(Stream.of("scope/s1")));
        assertEquals(getStreamCut("s2"), cfg.getStartingStreamCuts().get(Stream.of("scope/s2")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidAutoTruncateAtLastCheckpoint() {
        ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .stream(Stream.of(SCOPE, "s2"), getStreamCut("s2"))
                .autoPublishCheckpoint()
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStreamNames() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidStartStreamCut() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream("scope/s1", getStreamCut("s2"))
                         .stream("scope/s2", getStreamCut("s1"))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidStartStreamCutForStream() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s2"))
                         .stream("scope2/s2", getStreamCut("s1"))
                         .build();
    }

    @Test
    public void testValidStartAndEndStreamCuts() {
        ReaderGroupConfig.builder()
                         .disableAutomaticCheckpoints()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1"), StreamCut.UNBOUNDED)
                         .stream(Stream.of(SCOPE, "s2"), StreamCut.UNBOUNDED, getStreamCut("s2"))
                         .stream(Stream.of(SCOPE, "s3"))
                         .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEndStreamCutForStream() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), StreamCut.UNBOUNDED, getStreamCut("s2"))
                         .build();
    }

    @Test
    public void testInvalidStartAndEndStreamCuts() {
        assertThrows("Overlapping segments: Start segment offset cannot be greater than end segment offset",
                     () ->  ReaderGroupConfig.builder()
                                             .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", 15L), getStreamCut("s1", 10L))
                                             .build(),
                     t -> t instanceof IllegalArgumentException);
        assertThrows("Overlapping segments: End segment offset cannot be any value other than -1L in case the segment is completed in start StreaCut",
                     () ->  ReaderGroupConfig.builder()
                                             .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", -1L), getStreamCut("s1", 10L))
                                             .build(),
                     t -> t instanceof IllegalArgumentException);
    }

    @Test
    public void testValidStartAndEndStreamCutsWithSimilarSegments() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", 10L), getStreamCut("s1", 15L))
                         .build();
        // both start and end StreamCut point to a completed Segment -1L
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", -1L), getStreamCut("s1", -1L))
                         .build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartAndEndStreamCutsWithOverlap() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", 1, 2, 3),
                                 getStreamCut("s1", 0, 7))
                         .build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartAndEndStreamCutsWithPartialOverlap() {
        ReaderGroupConfig.builder()
                         .stream(Stream.of(SCOPE, "s1"), getStreamCut("s1", 0, 7),
                                 getStreamCut("s1", 5, 4, 6))
                         .build();
    }

    @Test
    public void testOutstandingCheckpointRequestConfig() {
        //test if the supplied pending checkpoint value is configured
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .maxOutstandingCheckpointRequest(5)
                .build();
        assertEquals(cfg.getMaxOutstandingCheckpointRequest(), 5);

        //test if the default checkpoint value is configured
        cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .build();
        assertEquals(cfg.getMaxOutstandingCheckpointRequest(), 3);

        //test if the minimum checkpoint value is being validated
        try {
            cfg = ReaderGroupConfig.builder()
                    .disableAutomaticCheckpoints()
                    .stream("scope/s1", getStreamCut("s1"))
                    .maxOutstandingCheckpointRequest(-1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Outstanding checkpoint request should be greater than zero");
        }

    }

    private StreamCut getStreamCut(String streamName) {
        return getStreamCut(streamName, 10L);
    }

    private StreamCut getStreamCut(String streamName, int...segments) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segments).forEach(seg -> builder.put(new Segment(SCOPE, streamName, seg), 10L));

        return new StreamCutImpl(Stream.of(SCOPE, streamName), builder.build());
    }

    private StreamCut getStreamCut(String streamName, long offset) {
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(SCOPE,
                streamName, 0), offset).build();
        return new StreamCutImpl(Stream.of(SCOPE, streamName), positions);
    }


    // Versioned serializer to simulate version 0 of ReaderGroupConfigSerializerV0
    private static class ReaderGroupConfigSerializerV0 extends VersionedSerializer.Direct<ReaderGroupConfig> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, ReaderGroupConfig builder) throws IOException {
            //NOP
        }

        private void write00(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(object.getAutomaticCheckpointIntervalMillis());
            revisionDataOutput.writeLong(object.getGroupRefreshTimeMillis());
            RevisionDataOutput.ElementSerializer<Stream> keySerializer = (out, s) -> out.writeUTF(s.getScopedName());
            RevisionDataOutput.ElementSerializer<StreamCut> valueSerializer = (out, cut) -> out.writeBuffer(new ByteArraySegment(cut.toBytes()));
            revisionDataOutput.writeMap(object.getStartingStreamCuts(), keySerializer, valueSerializer);
            revisionDataOutput.writeMap(object.getEndingStreamCuts(), keySerializer, valueSerializer);
        }
    }

    @Test
    public void testReaderGroupConfigSerializationCompatabilityV0() throws Exception {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", getStreamCut("s1"))
                .stream(Stream.of(SCOPE, "s2"), getStreamCut("s2"))
                .build();

        // Obtain version 0 serialized data
        final ByteBuffer bufV0 = ByteBuffer.wrap(new ReaderGroupConfigSerializerV0().serialize(cfg).array());
        // deserialize it using current version 1 serialization and ensure compatibility.
        assertEquals(cfg, ReaderGroupConfig.fromBytes(bufV0));
    }
}
