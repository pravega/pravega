/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Segment;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CheckpointStateTest {

    @Test
    public void testCheckpointCompletes() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of("a", "b"));
        assertFalse(state.isCheckpointComplete("foo"));
        state.readerCheckpointed("foo", "a", ImmutableMap.of(getSegment("S1"), 1L));
        assertFalse(state.isCheckpointComplete("foo"));
        assertNull(state.getPositionsForCompletedCheckpoint("foo"));
        state.readerCheckpointed("foo", "b", ImmutableMap.of(getSegment("S2"), 2L));
        assertTrue(state.isCheckpointComplete("foo"));
        Map<Segment, Long> completedCheckpoint = state.getPositionsForCompletedCheckpoint("foo");
        assertNotNull(completedCheckpoint);
        assertEquals(ImmutableMap.of(getSegment("S1"), 1L, getSegment("S2"), 2L), completedCheckpoint);
    }

    @Test
    public void testGetCheckpointForReader() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of("a", "b"));
        assertEquals("foo", state.getCheckpointForReader("a"));
        assertEquals("foo", state.getCheckpointForReader("b"));
        assertEquals(null, state.getCheckpointForReader("c"));
        state.readerCheckpointed("foo", "a", Collections.emptyMap());
        assertEquals(null, state.getCheckpointForReader("a"));
        assertEquals("foo", state.getCheckpointForReader("b"));
        state.clearCheckpointsThrough("foo");
        assertEquals(null, state.getCheckpointForReader("a"));
        assertEquals(null, state.getCheckpointForReader("b"));
    }

    private Segment getSegment(String name) {
        return new Segment("ExampleScope", name, 0);
    }

}
