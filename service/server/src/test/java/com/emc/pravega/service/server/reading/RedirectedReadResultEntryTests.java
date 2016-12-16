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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

/**
 * Unit tests for RedirectedReadResultEntry.
 */
public class RedirectedReadResultEntryTests {
    /**
     * Tests the ability of the ReadResultEntry base class to adjust offsets when instructed so.
     */
    @Test
    public void testConstructor() {
        final long originalOffset = 123;
        final int originalLength = 321;
        final int positiveDelta = 8976;
        final int negativeDelta = -76;
        TestReadResultEntry baseEntry = new TestReadResultEntry(originalOffset, originalLength);

        AssertExtensions.assertThrows(
                "Constructor allowed changing to a negative offset.",
                () -> new RedirectedReadResultEntry(baseEntry, -originalOffset - 1),
                ex -> ex instanceof IllegalArgumentException);

        // Adjust up.
        RedirectedReadResultEntry redirectedEntry = new RedirectedReadResultEntry(baseEntry, positiveDelta);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after up-adjustment.", originalOffset + positiveDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after up-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Adjust down.
        redirectedEntry = new RedirectedReadResultEntry(baseEntry, negativeDelta);
        Assert.assertEquals("Unexpected value for getStreamSegmentOffset after down-adjustment.", originalOffset + negativeDelta, redirectedEntry.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected value for getRequestedReadLength after down-adjustment (no change expected).", originalLength, redirectedEntry.getRequestedReadLength());

        // Verify other properties are as they should.
        Assert.assertEquals("Unexpected value for getType.", baseEntry.getType(), redirectedEntry.getType());
        Assert.assertEquals("Unexpected value for getRequestedReadLength.", baseEntry.getRequestedReadLength(), redirectedEntry.getRequestedReadLength());
        Assert.assertEquals("Unexpected result for getContent.", baseEntry.getContent(), redirectedEntry.getContent());

        redirectedEntry.requestContent(Duration.ZERO);
        Assert.assertTrue("BaseEntry.getContent() was not completed when requestContent was invoked.", FutureHelpers.isSuccessful(baseEntry.getContent()));
    }

    private static class TestReadResultEntry extends ReadResultEntryBase {
        TestReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
            super(ReadResultEntryType.Cache, streamSegmentOffset, requestedReadLength);
        }

        @Override
        public void requestContent(Duration timeout) {
            this.getContent().complete(null);
        }

    }
}
