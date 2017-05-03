/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.client.stream.Segment;
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
    public void testCheckpointNoReaders() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of());
        assertTrue(state.isCheckpointComplete("foo"));
    }
    
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
