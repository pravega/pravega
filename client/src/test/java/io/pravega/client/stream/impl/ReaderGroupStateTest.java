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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ReaderGroupState.AcquireSegment;
import io.pravega.client.stream.impl.ReaderGroupState.AddReader;
import io.pravega.client.stream.impl.ReaderGroupState.SegmentCompleted;
import io.pravega.client.stream.impl.ReaderGroupState.UpdatingConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupStateTest {

    private static final String SCOPE = "scope";
    @Mock
    private Revision revision;
    @Mock
    private ReaderGroupConfig readerConf;
    private ReaderGroupState readerState;

    @Before
    public void setup() {
        Map<SegmentWithRange, Long> offsetMap = new HashMap<>();
        offsetMap.put(new SegmentWithRange(new Segment(SCOPE, "S1", 0), 0, 1), 1L);
        offsetMap.put(new SegmentWithRange(new Segment(SCOPE, "S2", 0), 0, 1), 1L);
        readerState = new ReaderGroupState("stream", revision, readerConf, offsetMap, Collections.emptyMap(), false);
    }
    

    @Test
    public void getRanking() throws Exception {
        assertTrue(readerState.getOnlineReaders().isEmpty());
        AddReader addR1 = new ReaderGroupState.AddReader("r1");
        addR1.applyTo(readerState, revision);
        assertEquals(1, readerState.getOnlineReaders().size());
        AddReader addR2 = new ReaderGroupState.AddReader("r2");
        addR2.applyTo(readerState, revision);
        assertEquals(2, readerState.getOnlineReaders().size());
        new ReaderGroupState.AcquireSegment("r1", getSegment("S1")).applyTo(readerState, revision);
        SegmentWithRange s1r = new SegmentWithRange(getSegment("S1"), 0, 1);
        ImmutableMap<SegmentWithRange, Long> positions = ImmutableMap.of(s1r, 123L);
        new ReaderGroupState.UpdateDistanceToTail("r1", 1, positions).applyTo(readerState, revision);
        assertEquals(Collections.singleton(getSegment("S1")), readerState.getSegments("r1"));
        assertEquals(0, readerState.getRanking("r1"));
        assertEquals(1, readerState.getRanking("r2"));
        assertEquals(123L, readerState.getLastReadPositions(getStream("S1")).get(s1r).longValue());
        new ReaderGroupState.AcquireSegment("r1", getSegment("S2")).applyTo(readerState, revision);
        assertEquals(2, readerState.getSegments("r1").size());
        assertEquals(0, readerState.getRanking("r1"));
        assertEquals(1, readerState.getRanking("r2"));
        new ReaderGroupState.ReleaseSegment("r1", getSegment("S1"), 1).applyTo(readerState, revision);
        new ReaderGroupState.ReleaseSegment("r1", getSegment("S2"), 1).applyTo(readerState, revision);
        new ReaderGroupState.AcquireSegment("r2", getSegment("S1")).applyTo(readerState, revision);
        new ReaderGroupState.AcquireSegment("r2", getSegment("S2")).applyTo(readerState, revision);
        SegmentWithRange s2r = new SegmentWithRange(getSegment("S2"), 0, 1);
        positions = ImmutableMap.of(s2r, 123L);
        new ReaderGroupState.UpdateDistanceToTail("r2", 1, positions).applyTo(readerState, revision);
        assertEquals(0, readerState.getSegments("r1").size());
        assertEquals(1, readerState.getRanking("r1"));
        assertEquals(0, readerState.getRanking("r2"));
        assertEquals(1L, readerState.getLastReadPositions(getStream("S1")).get(s1r).longValue());
        assertEquals(123L, readerState.getLastReadPositions(getStream("S2")).get(s2r).longValue());
    }
    
    @Test
    public void testLastReadPositions() {
        Map<SegmentWithRange, Long> p1 = readerState.getLastReadPositions(Stream.of(SCOPE, "S1"));
        assertEquals(1, p1.size());
        Segment s1 = new Segment(SCOPE, "S1", 0);
        SegmentWithRange sr1 = new SegmentWithRange(s1, 0, 1);
        assertEquals(Long.valueOf(1), p1.get(sr1));
        Map<SegmentWithRange, Long> p2 = readerState.getLastReadPositions(Stream.of(SCOPE, "S2"));
        assertEquals(1, p2.size());
        Segment s2 = new Segment(SCOPE, "S2", 0);
        SegmentWithRange sr2 = new SegmentWithRange(s2, 0, 1);
        assertEquals(Long.valueOf(1), p2.get(sr2));
        
        AddReader addR1 = new ReaderGroupState.AddReader("r1");
        addR1.applyTo(readerState, revision);
        AcquireSegment aquire = new ReaderGroupState.AcquireSegment("r1", getSegment("S1"));
        aquire.applyTo(readerState, revision);
        SegmentWithRange sr3 = new SegmentWithRange(new Segment(SCOPE, "S1", 1), 0, 1);
        ImmutableMap<SegmentWithRange, List<Long>> successors = ImmutableMap.of(sr3, ImmutableList.of(0L));
        SegmentCompleted completed = new ReaderGroupState.SegmentCompleted("r1", sr1, successors);
        completed.applyTo(readerState, revision);
        
        p1 = readerState.getLastReadPositions(Stream.of(SCOPE, "S1"));
        assertEquals(1, p1.size());
        assertEquals(Long.valueOf(0), p1.get(sr3));
    }
    

    @Test
    public void getPositionsForLastCompletedCheckpointSuccess() throws Exception {
        CheckpointState chkPointState = readerState.getCheckpointState();
        chkPointState.beginNewCheckpoint("chk1",
                ImmutableSet.of("r1", "r2"), getOffsetMap(asList("S1", "S2"), 0L));
        chkPointState.readerCheckpointed("chk1", "r1", getOffsetMap(singletonList("S1"), 1L));
        chkPointState.readerCheckpointed("chk1", "r2", getOffsetMap(singletonList("S2"), 2L));

        Optional<Map<Stream, Map<Segment, Long>>> latestPosition = readerState.getPositionsForLastCompletedCheckpoint();

        assertTrue(latestPosition.isPresent());
        assertEquals(1L, latestPosition.get().get(getStream("S1")).get(getSegment("S1")).longValue());
        assertEquals(2L, latestPosition.get().get(getStream("S2")).get(getSegment("S2")).longValue());

        //add new checkpoint and verify for last checkpoint
        chkPointState.beginNewCheckpoint("chk2",
                ImmutableSet.of("r1", "r2"), getOffsetMap(asList("S1", "S2"), 4L));
        chkPointState.readerCheckpointed("chk2", "r1", getOffsetMap(singletonList("S1"), 5L));
        chkPointState.readerCheckpointed("chk2", "r2", getOffsetMap(singletonList("S2"), 6L));
        Optional<Map<Stream, Map<Segment, Long>>> latestPosition2 = readerState.getPositionsForLastCompletedCheckpoint();

        assertTrue(latestPosition2.isPresent());
        assertEquals(5L, latestPosition2.get().get(getStream("S1")).get(getSegment("S1")).longValue());
        assertEquals(6L, latestPosition2.get().get(getStream("S2")).get(getSegment("S2")).longValue());

    }

    @Test
    public void getPositionsForLastCompletedCheckpointFailure() throws Exception {
        CheckpointState chkPointState = readerState.getCheckpointState();

        //incomplete checkpoint.
        chkPointState.beginNewCheckpoint("incompletechkpoint",
                ImmutableSet.of("r1", "r2"), getOffsetMap(asList("S1", "S2"), 0L));
        chkPointState.readerCheckpointed("incompletechkpoint", "r1", getOffsetMap(singletonList("S1"), 1L));

        Optional<Map<Stream, Map<Segment, Long>>> latestPosition = readerState.getPositionsForLastCompletedCheckpoint();
        assertFalse("Incomplete checkpoint", latestPosition.isPresent());

        chkPointState.beginNewCheckpoint("chk1",
                ImmutableSet.of("r1", "r2"), getOffsetMap(asList("S1", "S2"), 0L));
        chkPointState.readerCheckpointed("chk1", "r1", getOffsetMap(singletonList("S1"), 3L));
        chkPointState.readerCheckpointed("chk1", "r2", getOffsetMap(singletonList("S2"), 3L));

        latestPosition = readerState.getPositionsForLastCompletedCheckpoint();
        assertTrue(latestPosition.isPresent());
        assertEquals(3L, latestPosition.get().get(getStream("S1")).get(getSegment("S1")).longValue());
        assertEquals(3L, latestPosition.get().get(getStream("S2")).get(getSegment("S2")).longValue());
    }

    @Test
    public void getStreamNames() {
        // configured Streams.
        Set<String> configuredStreams = ImmutableSet.of(getStream("S1").getScopedName(), getStream("S2").getScopedName());

        // validate stream names
        assertEquals(configuredStreams, readerState.getStreamNames());

        //Simulate addition of a reader and assigning of segments to the reader.
        new AddReader("reader1").applyTo(readerState, revision);
        new ReaderGroupState.AcquireSegment("reader1", new Segment(SCOPE, "S1", 0)).applyTo(readerState, revision);

        // validate stream names
        assertEquals(configuredStreams, readerState.getStreamNames());
    }

    @Test
    public void getStreamCutsForCompletedCheckpoint() {
        // Begin Checkpoint.
        CheckpointState chkPointState = readerState.getCheckpointState();
        chkPointState.beginNewCheckpoint("chk1",
                                         ImmutableSet.of("r1", "r2"), getOffsetMap(asList("S1", "S2"), 0L));

        // Simulate checkpointing for every reader.
        Map<Segment, Long> s1OffsetMap = getOffsetMap(singletonList("S1"), 1L);
        Map<Segment, Long> s2OffsetMap = getOffsetMap(singletonList("S2"), 2L);
        chkPointState.readerCheckpointed("chk1", "r1", s1OffsetMap);
        chkPointState.readerCheckpointed("chk1", "r2", s2OffsetMap);

        // Expected streamCuts.
        Map<Stream, StreamCut> expectedStreamCuts = ImmutableMap.<Stream, StreamCut>builder()
                .put(getStream("S1"), new StreamCutImpl(getStream("S1"), s1OffsetMap))
                .put(getStream("S2"), new StreamCutImpl(getStream("S2"), s2OffsetMap))
                .build();

        // invoke and verify.
        Optional<Map<Stream, StreamCut>> streamCuts = readerState.getStreamCutsForCompletedCheckpoint("chk1");
        assertTrue(streamCuts.isPresent());
        assertEquals(expectedStreamCuts, streamCuts.get());
    }

    @Test
    public void getStreamCutsForCompletedCheckpointMultipleScope() {

        // Begin checkpoint.
        Map<Segment, Long> offsetMap = getOffsetMap(asList("scope1", "scope2"), asList("s1", "s2"), 0L);
        CheckpointState chkPointState = readerState.getCheckpointState();
        chkPointState.beginNewCheckpoint("chk1", ImmutableSet.of("r1"), offsetMap);
        chkPointState.readerCheckpointed("chk1", "r1", getOffsetMap(asList("scope1", "scope2"), asList("s1", "s2"), 99L));

        Map<Stream, StreamCut> expectedStreamCuts = getStreamCutMap(asList("scope1", "scope2"), asList("s1", "s2"), 99L);

        // invoke and verify.
        Optional<Map<Stream, StreamCut>> streamCuts = readerState.getStreamCutsForCompletedCheckpoint("chk1");
        assertTrue(streamCuts.isPresent());
        assertEquals(expectedStreamCuts, streamCuts.get());
    }

    @Test
    public void testUpdatingConfig() {
        UpdatingConfig update1 = new UpdatingConfig(true);
        update1.applyTo(readerState, revision);
        assertTrue(readerState.isUpdatingConfig());

        UpdatingConfig update2 = new UpdatingConfig(false);
        update2.applyTo(readerState, revision);
        assertFalse(readerState.isUpdatingConfig());
    }

    private Segment getSegment(String streamName) {
        return new Segment(SCOPE, streamName, 0);
    }

    private Stream getStream(String streamName) {
        return new StreamImpl(SCOPE, streamName);
    }

    private Map<Segment, Long> getOffsetMap(List<String> streamNames, long offset) {
       return getOffsetMap(Collections.singletonList(SCOPE), streamNames, offset);
    }

    private Map<Segment, Long> getOffsetMap(List<String> scopes, List<String> streams, long offset) {
        Map<Segment, Long> offsetMap = new HashMap<>();
        scopes.forEach(scope -> streams.forEach(stream -> offsetMap.put(new Segment(scope, stream, 0), offset)));
        return offsetMap;

    }

    private Map<Stream, StreamCut> getStreamCutMap(List<String> scopes, List<String> streams, long offset) {
        Map<Stream, StreamCut> map = new HashMap<>();
        scopes.forEach(scope -> streams.forEach(stream -> map.put(Stream.of(scope, stream),
                                                                  new StreamCutImpl(Stream.of(scope, stream),
                                                                                    getOffsetMap(singletonList(scope),
                                                                                                 singletonList(stream),
                                                                                                 offset)))));
        return map;
    }
}
