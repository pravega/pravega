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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.BufferView;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link DeltaIteratorState} class. It tests for constructor(s) correctness and the serialization/deserialization process.
 */
@Slf4j
public class DeltaIteratorStateTests {

    @Test
    public void testConstructors() {
        DeltaIteratorState args = new DeltaIteratorState(0, true, false, false);
        DeltaIteratorState noArgs = new DeltaIteratorState();

        Assert.assertTrue(args.getFromPosition() == noArgs.getFromPosition());
        Assert.assertTrue(args.isReachedEnd() == noArgs.isReachedEnd());
        Assert.assertTrue(args.isDeletionRecord() == noArgs.isDeletionRecord());
        Assert.assertTrue(args.isShouldClear() == noArgs.isShouldClear());
    }

    @Test
    public void testPosition() {
        AssertExtensions.assertThrows(
                "Invalid position provided.",
                () -> new DeltaIteratorState(-1, true, true, true),
                ex -> ex instanceof IllegalArgumentException
        );
        DeltaIteratorState valid = new DeltaIteratorState(1, true, true, true);
        Assert.assertTrue(DeltaIteratorState.isValid(valid.getFromPosition()));
    }

    @Test
    public void testSerialization() {
        DeltaIteratorState one = new DeltaIteratorState(1, true, false, true);
        DeltaIteratorState two = new DeltaIteratorState(1, true, false, true);
        BufferView bufferOne = one.serialize();
        BufferView bufferTwo = two.serialize();
        // Make sure that two state objects with identical arguments get serialized to same byte array.
        Assert.assertArrayEquals(bufferOne.getCopy(), bufferTwo.getCopy());
        // Check that they are now deserialized back to their original form.
        DeltaIteratorState stateOne = DeltaIteratorState.deserialize(bufferOne);
        DeltaIteratorState stateTwo = DeltaIteratorState.deserialize(bufferTwo);
        Assert.assertTrue(testEquals(stateOne, one));
        Assert.assertTrue(testEquals(stateTwo, two));
        // Also check that they are deserialized back into equal objects.
        Assert.assertTrue(testEquals(stateOne, stateTwo));
    }

    private boolean testEquals(DeltaIteratorState a, DeltaIteratorState b) {
        return a.getFromPosition() == b.getFromPosition() &&
                a.isShouldClear() == b.isShouldClear() &&
                a.isDeletionRecord() == b.isDeletionRecord() &&
                a.isReachedEnd() == b.isReachedEnd();
    }
}
