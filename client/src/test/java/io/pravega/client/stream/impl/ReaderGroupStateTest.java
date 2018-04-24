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

import com.google.common.collect.ImmutableSet;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ReaderGroupState.AddReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
        readerState = new ReaderGroupState("stream", revision, readerConf,
                getOffsetMap(1L, Arrays.asList("S1", "S2")), Collections.emptyMap());
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
        new ReaderGroupState.UpdateDistanceToTail("r1", 1).applyTo(readerState, revision);
        assertEquals(Collections.singleton(getSegment("S1")), readerState.getSegments("r1"));
        assertEquals(0, readerState.getRanking("r1"));
        assertEquals(1, readerState.getRanking("r2"));
        new ReaderGroupState.AcquireSegment("r1", getSegment("S2")).applyTo(readerState, revision);
        assertEquals(2, readerState.getSegments("r1").size());
        assertEquals(0, readerState.getRanking("r1"));
        assertEquals(1, readerState.getRanking("r2"));
        new ReaderGroupState.ReleaseSegment("r1", getSegment("S1"), 1).applyTo(readerState, revision);
        new ReaderGroupState.ReleaseSegment("r1", getSegment("S2"), 1).applyTo(readerState, revision);
        new ReaderGroupState.AcquireSegment("r2", getSegment("S1")).applyTo(readerState, revision);
        new ReaderGroupState.AcquireSegment("r2", getSegment("S2")).applyTo(readerState, revision);
        new ReaderGroupState.UpdateDistanceToTail("r2", 1).applyTo(readerState, revision);
        assertEquals(0, readerState.getSegments("r1").size());
        assertEquals(1, readerState.getRanking("r1"));
        assertEquals(0, readerState.getRanking("r2"));
    }
    

    @Test
    public void getPositionsForLastCompletedCheckpointSuccess() throws Exception {
        CheckpointState chkPointState = readerState.getCheckpointState();
        chkPointState.beginNewCheckpoint("chk1",
                ImmutableSet.of("r1", "r2"), getOffsetMap(0L, Arrays.asList("S1", "S2")));
        chkPointState.readerCheckpointed("chk1", "r1", getOffsetMap(1L, singletonList("S1")));
        chkPointState.readerCheckpointed("chk1", "r2", getOffsetMap(2L, singletonList("S2")));

        Optional<Map<Stream, Map<Segment, Long>>> latestPosition = readerState.getPositionsForLastCompletedCheckpoint();
        assertTrue(latestPosition.isPresent());
        assertEquals(1L, latestPosition.get().get(getStream("S1")).get(getSegment("S1")).longValue());
        assertEquals(2L, latestPosition.get().get(getStream("S2")).get(getSegment("S2")).longValue());
    }

    @Test
    public void getPositionsForLastCompletedCheckpointFailure() throws Exception {
        CheckpointState chkPointState = readerState.getCheckpointState();

        //incomplete checkpoint.
        chkPointState.beginNewCheckpoint("incompletechkpoint",
                ImmutableSet.of("r1", "r2"), getOffsetMap(0L, Arrays.asList("S1", "S2")));
        chkPointState.readerCheckpointed("incompletechkpoint", "r1", getOffsetMap(1L, singletonList("S1")));

        Optional<Map<Stream, Map<Segment, Long>>> latestPosition = readerState.getPositionsForLastCompletedCheckpoint();
        assertFalse("Incomplete checkpoint", latestPosition.isPresent());

        chkPointState.beginNewCheckpoint("chk1",
                ImmutableSet.of("r1", "r2"), getOffsetMap(0L, Arrays.asList("S1", "S2")));
        chkPointState.readerCheckpointed("chk1", "r1", getOffsetMap(3L, singletonList("S1")));
        chkPointState.readerCheckpointed("chk1", "r2", getOffsetMap(3L, singletonList("S2")));

        latestPosition = readerState.getPositionsForLastCompletedCheckpoint();
        assertTrue(latestPosition.isPresent());
        assertEquals(3L, latestPosition.get().get(getStream("S1")).get(getSegment("S1")).longValue());
        assertEquals(3L, latestPosition.get().get(getStream("S2")).get(getSegment("S2")).longValue());
    }

    private Segment getSegment(String streamName) {
        return new Segment(SCOPE, streamName, 0);
    }

    private Stream getStream(String streamName) {
        return new StreamImpl(SCOPE, streamName);
    }

    private Map<Segment, Long> getOffsetMap(Long offset, List<String> names) {
        Map<Segment, Long> offsetMap = new HashMap<>();
        names.forEach(name -> offsetMap.put(getSegment(name), offset));
        return offsetMap;
    }
}
