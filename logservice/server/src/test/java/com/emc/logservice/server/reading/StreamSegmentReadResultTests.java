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

package com.emc.logservice.server.reading;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.contracts.ReadResultEntryType;
import com.emc.nautilus.testcommon.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for StreamSegmentReadResult class.
 */
public class StreamSegmentReadResultTests {
    private static final int START_OFFSET = 123456;
    private static final int MAX_RESULT_LENGTH = 1024;
    private static final int READ_ITEM_LENGTH = 1;

    /**
     * Tests the next() method which ends when the result is fully consumed (via offsets).
     */
    @Test
    public void testNextFullyConsumed() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, and return items, 1 byte at a time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        int expectedConsumedLength = 0;
        for (int i = 0; i < MAX_RESULT_LENGTH; i += READ_ITEM_LENGTH) {
            // Setup an item to be returned.
            final long expectedStartOffset = START_OFFSET + i;
            final int expectedReadLength = MAX_RESULT_LENGTH - i;
            nextEntry.set(new TestReadResultEntry(expectedStartOffset, expectedReadLength, false));

            // Get the result and verify we get exactly what we supplied.
            Assert.assertTrue("hasNext() returned false even though we haven't consumed the entire result.", r.hasNext());
            ReadResultEntry resultEntry = r.next();
            Assert.assertEquals("Unexpected result from nextEntry.", nextEntry.get(), resultEntry);

            // Verify the StreamSegmentReadResult does not update itself after returning a result.
            Assert.assertEquals("getStreamSegmentStartOffset changed while iterating.", START_OFFSET, r.getStreamSegmentStartOffset());
            Assert.assertEquals("getMaxResultLength changed while iterating.", MAX_RESULT_LENGTH, r.getMaxResultLength());
            Assert.assertEquals("Unexpected value from getConsumedLength after returning a value but before completing result future.", expectedConsumedLength, r.getConsumedLength());

            // Verify the StreamSegmentReadResult updates itself after the last returned result's future is completed.
            nextEntry.get().complete(new ReadResultEntryContents(null, READ_ITEM_LENGTH));
            expectedConsumedLength += READ_ITEM_LENGTH;
            Assert.assertEquals("Unexpected value from getConsumedLength after returning a value and completing result future.", expectedConsumedLength, r.getConsumedLength());
        }

        // Verify we have reached the end.
        Assert.assertEquals("Unexpected state of the StreamSegmentReadResult when consuming the entire result.", r.getMaxResultLength(), r.getConsumedLength());
        Assert.assertFalse("hasNext() did not return false when the entire result is consumed.", r.hasNext());
        ReadResultEntry resultEntry = r.next();
        Assert.assertNull("next() did not return null when it was done.", resultEntry);
    }

    /**
     * Tests the next() method which ends when the one of the returned items indicates the end of the Stream Segment,
     * even if the result is not fully consumed.
     */
    @Test
    public void testNextEndOfStreamSegment() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, and return only half the items, 1 byte at a time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        for (int i = 0; i < MAX_RESULT_LENGTH / 2; i++) {
            // Setup an item to be returned.
            final long expectedStartOffset = START_OFFSET + i;
            final int expectedReadLength = MAX_RESULT_LENGTH - i;
            nextEntry.set(new TestReadResultEntry(expectedStartOffset, expectedReadLength, false));
            r.next();
            nextEntry.get().complete(new ReadResultEntryContents(null, READ_ITEM_LENGTH));
        }

        // Verify we have not reached the end.
        AssertExtensions.assertLessThan("Unexpected state of the StreamSegmentReadResult when consuming half of the result.", r.getMaxResultLength(), r.getConsumedLength());
        Assert.assertTrue("hasNext() did not return true when more items are to be consumed.", r.hasNext());

        // Next time we call next(), return an End-of-StreamSegment entry.
        nextEntry.set(new TestReadResultEntry(START_OFFSET + MAX_RESULT_LENGTH / 2, MAX_RESULT_LENGTH / 2, true));
        ReadResultEntry resultEntry = r.next();
        Assert.assertEquals("Unexpected result from nextEntry() when returning the last item in a StreamSegment.", nextEntry.get(), resultEntry);
        Assert.assertFalse("hasNext() did not return false when reaching the end of a sealed StreamSegment.", r.hasNext());
        resultEntry = r.next();
        Assert.assertNull("next() did return null when it encountered the end of a sealed StreamSegment.", resultEntry);
    }

    /**
     * Tests the ability to close the result and cancel any items that were returned.
     */
    @Test
    public void testClose() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, but we only get to read one item from it.
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        nextEntry.set(new TestReadResultEntry(START_OFFSET, MAX_RESULT_LENGTH, false));
        ReadResultEntry resultEntry = r.next();

        // Close the result and verify we cannot read from it anymore and that the pending future is now canceled.
        r.close();
        Assert.assertTrue("Already returned result future is not canceled after closing the ReadResult.", resultEntry.getContent().isCancelled());
        Assert.assertFalse("hasNext() did not return false after closing ", r.hasNext());
        AssertExtensions.assertThrows(
                "next() did not throw an appropriate exception when the ReadResult is closed.",
                r::next,
                ex -> ex instanceof ObjectClosedException);
    }

    /**
     * Tests the ability to only return a next item if the previous returned item hasn't been consumed yet.
     */
    @Test
    public void testNextWaitOnPrevious() throws Exception {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length) -> nextEntry.get();

        // We issue a read, get one item, do not consume it, and then read a second time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        nextEntry.set(new TestReadResultEntry(START_OFFSET, MAX_RESULT_LENGTH, false));
        TestReadResultEntry firstEntry = (TestReadResultEntry) r.next();

        // Immediately request a second item, without properly consuming the first item.
        nextEntry.set(new TestReadResultEntry(START_OFFSET + READ_ITEM_LENGTH, MAX_RESULT_LENGTH, false));

        AssertExtensions.assertThrows(
                "Second read was allowed even though the first read did not complete.",
                r::next,
                ex -> ex instanceof IllegalStateException);

        firstEntry.complete(new ReadResultEntryContents(null, READ_ITEM_LENGTH));
        ReadResultEntry secondEntry = r.next();
        Assert.assertEquals("Unexpected result from nextEntry.", nextEntry.get(), secondEntry);
    }

    //region TestReadResultEntry

    private static class TestReadResultEntry extends ReadResultEntryBase {
        TestReadResultEntry(long streamSegmentOffset, int requestedReadLength, boolean endOfSegment) {
            super(endOfSegment ? ReadResultEntryType.EndOfStreamSegment : ReadResultEntryType.Cache, streamSegmentOffset, requestedReadLength);
        }

        @Override
        public void complete(ReadResultEntryContents entryContents) {
            super.complete(entryContents);
        }

        @Override
        public void fail(Throwable ex) {
            super.fail(ex);
        }
    }

    //endregion
}
