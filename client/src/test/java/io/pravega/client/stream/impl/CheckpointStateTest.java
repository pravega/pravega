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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.segment.impl.Segment;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

import static io.pravega.client.stream.impl.ReaderGroupImpl.SILENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CheckpointStateTest {

    @Test
    public void testCheckpointNoReaders() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of(), Collections.emptyMap());
        assertTrue(state.isCheckpointComplete("foo"));
        assertFalse(state.getPositionsForLatestCompletedCheckpoint().isPresent());
    }
    
    @Test
    public void testCheckpointCompletes() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of("a", "b"), Collections.emptyMap());
        assertFalse(state.isCheckpointComplete("foo"));
        state.readerCheckpointed("foo", "a", ImmutableMap.of(getSegment("S1"), 1L));
        assertFalse(state.isCheckpointComplete("foo"));
        assertNull(state.getPositionsForCompletedCheckpoint("foo"));
        state.readerCheckpointed("foo", "b", ImmutableMap.of(getSegment("S2"), 2L));
        assertTrue(state.isCheckpointComplete("foo"));
        Map<Segment, Long> completedCheckpoint = state.getPositionsForCompletedCheckpoint("foo");
        assertNotNull(completedCheckpoint);
        assertEquals(ImmutableMap.of(getSegment("S1"), 1L, getSegment("S2"), 2L), completedCheckpoint);
        state.clearCheckpointsBefore("foo");
        assertEquals(ImmutableMap.of(getSegment("S1"), 1L, getSegment("S2"), 2L),
                state.getPositionsForLatestCompletedCheckpoint().get());
    }

    @Test
    public void testGetCheckpointForReader() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("foo", ImmutableSet.of("a", "b"), Collections.emptyMap());
        assertEquals("foo", state.getCheckpointForReader("a"));
        assertEquals("foo", state.getCheckpointForReader("b"));
        assertEquals(null, state.getCheckpointForReader("c"));
        state.readerCheckpointed("foo", "a", Collections.emptyMap());
        assertEquals(null, state.getCheckpointForReader("a"));
        assertEquals("foo", state.getCheckpointForReader("b"));
        state.clearCheckpointsBefore("foo");
        assertEquals(null, state.getCheckpointForReader("a"));
        assertEquals("foo", state.getCheckpointForReader("b"));
        assertFalse(state.getPositionsForLatestCompletedCheckpoint().isPresent());
    }
    
    @Test
    public void testCheckpointsCleared() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("1", ImmutableSet.of("a", "b"), Collections.emptyMap());
        state.beginNewCheckpoint("2", ImmutableSet.of("a", "b"), Collections.emptyMap());
        state.beginNewCheckpoint("3", ImmutableSet.of("a", "b"), Collections.emptyMap());
        assertEquals("1", state.getCheckpointForReader("a"));
        assertEquals("1", state.getCheckpointForReader("b"));
        assertEquals(null, state.getCheckpointForReader("c"));
        state.readerCheckpointed("1", "a", Collections.emptyMap());
        assertEquals("2", state.getCheckpointForReader("a"));
        assertEquals("1", state.getCheckpointForReader("b"));
        state.clearCheckpointsBefore("2");
        assertEquals("2", state.getCheckpointForReader("a"));
        assertEquals("2", state.getCheckpointForReader("b"));
        state.clearCheckpointsBefore("3");
        assertEquals("3", state.getCheckpointForReader("a"));
        assertEquals("3", state.getCheckpointForReader("b"));
        assertFalse(state.getPositionsForLatestCompletedCheckpoint().isPresent());
    }

    @Test
    public void testRemoveOutstandingCheckpointsCleared() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("1", ImmutableSet.of("a", "b"), Collections.emptyMap());
        state.beginNewCheckpoint("2", ImmutableSet.of("a", "b"), Collections.emptyMap());
        state.beginNewCheckpoint("3", ImmutableSet.of("a", "b"), Collections.emptyMap());
        assertEquals("1", state.getCheckpointForReader("a"));
        assertEquals("1", state.getCheckpointForReader("b"));
        assertEquals(null, state.getCheckpointForReader("c"));
        state.readerCheckpointed("1", "a", Collections.emptyMap());
        assertEquals("2", state.getCheckpointForReader("a"));
        assertEquals("1", state.getCheckpointForReader("b"));
        assertEquals(3, state.getOutstandingCheckpoints().size());
        state.removeOutstandingCheckpoints();
        assertEquals(0, state.getOutstandingCheckpoints().size());
    }

    @Test
    public void testOutstandingCheckpoint() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("1", ImmutableSet.of("a"), Collections.emptyMap());
        state.beginNewCheckpoint("2", ImmutableSet.of("a"), Collections.emptyMap());
        state.beginNewCheckpoint("3" + SILENT, ImmutableSet.of("a"), Collections.emptyMap());
        state.beginNewCheckpoint("4", ImmutableSet.of("a"), Collections.emptyMap());
        // Silent checkpoint should not be counted as part of CheckpointState#getOutstandingCheckpoints.
        assertEquals(3, state.getOutstandingCheckpoints().size());

        //Complete checkpoint "2"
        state.readerCheckpointed("2", "a", ImmutableMap.of(getSegment("S1"), 1L));
        assertTrue(state.isCheckpointComplete("2"));
        assertEquals( ImmutableMap.of(getSegment("S1"), 1L), state.getPositionsForCompletedCheckpoint("2"));
        state.clearCheckpointsBefore("2");
        // All check points before checkpoint id "2" are completed.
        assertTrue(state.isCheckpointComplete("1"));
        // Only checkpoint "4" is outstanding as checkpoints "1" and "2" are complete and silent checkpoints are ignored.
        assertEquals(1, state.getOutstandingCheckpoints().size());

        state.readerCheckpointed("3" + SILENT, "a", Collections.emptyMap());
        assertTrue(state.isCheckpointComplete("4" + SILENT));
        assertEquals(1, state.getOutstandingCheckpoints().size()); // Checkpoint 4 is outstanding.

        state.readerCheckpointed("4", "a", Collections.emptyMap());
        assertTrue(state.isCheckpointComplete("4"));
        assertEquals(0, state.getOutstandingCheckpoints().size());
    }

    @Test
    public void testLastCheckpointPosition() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("1", ImmutableSet.of("a"), Collections.emptyMap());
        state.beginNewCheckpoint("2" + SILENT, ImmutableSet.of("a"), Collections.emptyMap());
        assertTrue(state.isLastCheckpointPublished());
        //Complete checkpoint "1"
        state.readerCheckpointed("1", "a", ImmutableMap.of(getSegment("S1"), 1L));
        assertTrue(state.isCheckpointComplete("1"));
        // Assert this is set to false by the last reader
        assertFalse(state.isLastCheckpointPublished());
        assertEquals(ImmutableMap.of(getSegment("S1"), 1L), state.getPositionsForCompletedCheckpoint("1"));
        state.clearCheckpointsBefore("1");

        //Complete silent checkpoint
        state.readerCheckpointed("2" + SILENT, "a", ImmutableMap.of(getSegment("S1"), 3L));
        assertTrue(state.isCheckpointComplete("2" + SILENT));
        assertEquals(ImmutableMap.of(getSegment("S1"), 3L), state.getPositionsForCompletedCheckpoint("2" + SILENT));
        state.clearCheckpointsBefore("2" + SILENT);

        // The last checkpoint position should contain the positions of the last checkpoint not stream-cut/silent checkpoint
        Map<Segment, Long> lastCheckpointPosition = null;
        if (state.getPositionsForLatestCompletedCheckpoint().isPresent()) {
            lastCheckpointPosition = state.getPositionsForLatestCompletedCheckpoint().get();
        }

        // Last checkpoint position is the same as checkpoint "1" positions
        assertEquals(lastCheckpointPosition, ImmutableMap.of(getSegment("S1"), 1L));

        // Last checkpoint position is not the same as silent checkpoint positions
        assertNotEquals(lastCheckpointPosition, ImmutableMap.of(getSegment("S1"), 3L));
    }

    @Test
    public void testGetReaderBlockingCheckpointsMap() {
        CheckpointState state = new CheckpointState();
        state.beginNewCheckpoint("1", ImmutableSet.of("a", "b"), Collections.emptyMap());
        state.beginNewCheckpoint("2" + SILENT, ImmutableSet.of("a"), Collections.emptyMap());
        state.beginNewCheckpoint("3", ImmutableSet.of("a", "b"), Collections.emptyMap());
        // Silent checkpoint should not be counted as part of CheckpointState#getOutstandingCheckpoints.

        assertEquals(2, state.getReaderBlockingCheckpointsMap().size());

        // Complete checkpoint "1" by reader 'a'.
        state.readerCheckpointed("1", "a", ImmutableMap.of(getSegment("S1"), 1L));
        // Checkpoint "1" is not completed by reader "b"
        assertEquals("b", state.getReaderBlockingCheckpointsMap().get("1").get(0));
        assertFalse(state.isCheckpointComplete("1"));
        // CheckpointState#getReaderBlockingCheckpointsMap contains 2 entries
        assertEquals(2, state.getReaderBlockingCheckpointsMap().size());

        // Complete checkpoint "1" by reader 'b'.
        state.readerCheckpointed("1", "b", ImmutableMap.of(getSegment("S1"), 1L));

        // Only checkpoint "3" is outstanding as checkpoints "1" is complete and silent checkpoints are ignored.
        assertEquals(1, state.getReaderBlockingCheckpointsMap().size());
        assertTrue(state.isCheckpointComplete("1"));
        assertFalse(state.isCheckpointComplete("3"));
    }

    private Segment getSegment(String name) {
        return new Segment("ExampleScope", name, 0);
    }

}
