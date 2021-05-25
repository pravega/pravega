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
package io.pravega.client.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.CheckpointImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
    public void testEquals() {
        ReaderGroupConfig cfg1 = ReaderGroupConfig.builder()
                                                  .stream(Stream.of(SCOPE, "s1"), StreamCut.UNBOUNDED)
                                                  .build();

        ReaderGroupConfig cfg2 = ReaderGroupConfig.cloneConfig(cfg1, UUID.randomUUID(), 100L);
        assertEquals(cfg1, cfg2);
        ReaderGroupConfig cfg3 = ReaderGroupConfig.builder()
                                                  .stream(Stream.of(SCOPE, "s1"), StreamCut.UNBOUNDED)
                                                  .stream(Stream.of(SCOPE, "s2"), StreamCut.UNBOUNDED)
                                                  .build();
        assertNotEquals(cfg2, cfg3);
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
}
