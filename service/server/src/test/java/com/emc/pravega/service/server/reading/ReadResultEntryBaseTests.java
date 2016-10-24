/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ReadResultEntryBase.
 */
public class ReadResultEntryBaseTests {
    /**
     * Tests the ability of the ReadResultEntry base class to adjust offsets when instructed so.
     */
    @Test
    public void testAdjustOffset() {
        final long originalOffset = 123;
        final int originalLength = 321;
        final int positiveDelta = 8976;
        final int negativeDelta = 76;
        TestReadResultEntry e = new TestReadResultEntry(originalOffset, originalLength);
        AssertExtensions.assertThrows("adjustOffset allowed changing to a negative offset.", () -> e.adjustOffset(-originalOffset - 1), ex -> ex instanceof IllegalArgumentException);

        // Adjust up.
        e.adjustOffset(positiveDelta);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after up-adjustment.", originalOffset + positiveDelta, e.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after up-adjustment (no change expected).", originalLength, e.getRequestedReadLength());

        // Adjust down.
        e.adjustOffset(negativeDelta);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after down-adjustment.", originalOffset + positiveDelta + negativeDelta, e.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after down-adjustment (no change expected).", originalLength, e.getRequestedReadLength());
    }

    private static class TestReadResultEntry extends ReadResultEntryBase {
        public TestReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
            super(ReadResultEntryType.Cache, streamSegmentOffset, requestedReadLength);
        }
    }
}
