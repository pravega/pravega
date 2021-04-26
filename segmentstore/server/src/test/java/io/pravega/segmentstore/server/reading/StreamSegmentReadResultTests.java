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
package io.pravega.segmentstore.server.reading;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.test.common.AssertExtensions;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for StreamSegmentReadResult class.
 */
public class StreamSegmentReadResultTests {
    private static final int START_OFFSET = 123456;
    private static final int MAX_RESULT_LENGTH = 1024;
    private static final int READ_ITEM_LENGTH = 1;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the ability to properly set Copy-on-Read.
     */
    @Test
    public void testCopyOnRead() {
        AtomicBoolean expectedMakeCopy = new AtomicBoolean(true);
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> {
            Assert.assertEquals(expectedMakeCopy.get(), makeCopy);
            return TestReadResultEntry.endOfSegment(offset, length);
        };
        @Cleanup
        StreamSegmentReadResult r1 = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        r1.next();

        @Cleanup
        StreamSegmentReadResult r2 = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        r2.setCopyOnRead(false);
        expectedMakeCopy.set(false);
        r2.next();
    }

    /**
     * Tests the ability to handle {@link StreamSegmentReadResult#setMaxReadAtOnce(int)}.
     */
    @Test
    public void testMaxReadAtOnce() {
        final int maxReadAtOnce = 10;
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> TestReadResultEntry.endOfSegment(offset, length);

        // Set it to a small value.
        @Cleanup
        StreamSegmentReadResult r1 = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        r1.setMaxReadAtOnce(maxReadAtOnce);
        val r11 = r1.next();
        Assert.assertEquals(maxReadAtOnce, r11.getRequestedReadLength());

        // Set it to 0.
        @Cleanup
        StreamSegmentReadResult r2 = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        r2.setMaxReadAtOnce(0);
        val r21 = r2.next();
        Assert.assertEquals(MAX_RESULT_LENGTH, r21.getRequestedReadLength());

        // Set it to more than max result length.
        @Cleanup
        StreamSegmentReadResult r3 = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        r3.setMaxReadAtOnce(MAX_RESULT_LENGTH + 123);
        val r31 = r3.next();
        Assert.assertEquals(MAX_RESULT_LENGTH, r31.getRequestedReadLength());
    }

    /**
     * Tests the next() method which ends when the result is fully consumed (via offsets).
     */
    @Test
    public void testNextFullyConsumed() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, and return items, 1 byte at a time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        int expectedConsumedLength = 0;
        for (int i = 0; i < MAX_RESULT_LENGTH; i += READ_ITEM_LENGTH) {
            // Setup an item to be returned.
            final long expectedStartOffset = START_OFFSET + i;
            final int expectedReadLength = MAX_RESULT_LENGTH - i;
            nextEntry.set(TestReadResultEntry.cache(expectedStartOffset, expectedReadLength));

            // Get the result and verify we get exactly what we supplied.
            Assert.assertTrue("hasNext() returned false even though we haven't consumed the entire result.", r.hasNext());
            ReadResultEntry resultEntry = r.next();
            Assert.assertEquals("Unexpected result from nextEntry.", nextEntry.get(), resultEntry);

            // Verify the StreamSegmentReadResult does not update itself after returning a result.
            Assert.assertEquals("getStreamSegmentStartOffset changed while iterating.", START_OFFSET, r.getStreamSegmentStartOffset());
            Assert.assertEquals("getMaxResultLength changed while iterating.", MAX_RESULT_LENGTH, r.getMaxResultLength());
            Assert.assertEquals("Unexpected value from getConsumedLength after returning a value but before completing result future.", expectedConsumedLength, r.getConsumedLength());

            // Verify the StreamSegmentReadResult updates itself after the last returned result's future is completed.
            nextEntry.get().complete(new ByteArraySegment(new byte[READ_ITEM_LENGTH]));
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
        testNextTerminal(TestReadResultEntry::endOfSegment);
    }

    /**
     * Tests the next() method which ends when the one of the returned items indicates a truncated Stream Segment,
     * even if the result is not fully consumed.
     */
    @Test
    public void testNextTruncated() {
        testNextTerminal(TestReadResultEntry::truncated);
    }

    private void testNextTerminal(BiFunction<Long, Integer, TestReadResultEntry> terminalEntryCreator) {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, and return only half the items, 1 byte at a time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        for (int i = 0; i < MAX_RESULT_LENGTH / 2; i++) {
            // Setup an item to be returned.
            final long expectedStartOffset = START_OFFSET + i;
            final int expectedReadLength = MAX_RESULT_LENGTH - i;
            nextEntry.set(TestReadResultEntry.cache(expectedStartOffset, expectedReadLength));
            r.next();
            nextEntry.get().complete(new ByteArraySegment(new byte[READ_ITEM_LENGTH]));
        }

        // Verify we have not reached the end.
        AssertExtensions.assertLessThan("Unexpected state of the StreamSegmentReadResult when consuming half of the result.",
                r.getMaxResultLength(), r.getConsumedLength());
        Assert.assertTrue("hasNext() did not return true when more items are to be consumed.", r.hasNext());

        // Next time we call next(), return an End-of-StreamSegment entry.
        nextEntry.set(terminalEntryCreator.apply((long) START_OFFSET + MAX_RESULT_LENGTH / 2, MAX_RESULT_LENGTH / 2));
        ReadResultEntry resultEntry = r.next();
        Assert.assertEquals("Unexpected result from nextEntry() when returning the terminal item from the result.", nextEntry.get(), resultEntry);
        Assert.assertFalse("hasNext() did not return false when reaching a terminal state.", r.hasNext());
        resultEntry = r.next();
        Assert.assertNull("next() did return null when it encountered a terminal state.", resultEntry);
    }

    /**
     * Tests the ability to close the result and cancel any items that were returned.
     */
    @Test
    public void testClose() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> nextEntry.get();

        // We issue a read with length = MAX_RESULT_LENGTH, but we only get to read one item from it.
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        nextEntry.set(TestReadResultEntry.cache(START_OFFSET, MAX_RESULT_LENGTH));
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
    public void testNextWaitOnPrevious() {
        AtomicReference<TestReadResultEntry> nextEntry = new AtomicReference<>();
        StreamSegmentReadResult.NextEntrySupplier nes = (offset, length, makeCopy) -> nextEntry.get();

        // We issue a read, get one item, do not consume it, and then read a second time.
        @Cleanup
        StreamSegmentReadResult r = new StreamSegmentReadResult(START_OFFSET, MAX_RESULT_LENGTH, nes, "");
        nextEntry.set(TestReadResultEntry.cache(START_OFFSET, MAX_RESULT_LENGTH));
        TestReadResultEntry firstEntry = (TestReadResultEntry) r.next();

        // Immediately request a second item, without properly consuming the first item.
        nextEntry.set(TestReadResultEntry.cache(START_OFFSET + READ_ITEM_LENGTH, MAX_RESULT_LENGTH));

        AssertExtensions.assertThrows(
                "Second read was allowed even though the first read did not complete.",
                r::next,
                ex -> ex instanceof IllegalStateException);

        firstEntry.complete(new ByteArraySegment(new byte[READ_ITEM_LENGTH]));
        ReadResultEntry secondEntry = r.next();
        Assert.assertEquals("Unexpected result from nextEntry.", nextEntry.get(), secondEntry);
    }

    //region TestReadResultEntry

    private static class TestReadResultEntry extends ReadResultEntryBase {
        private TestReadResultEntry(long streamSegmentOffset, int requestedReadLength, ReadResultEntryType type) {
            super(type, streamSegmentOffset, requestedReadLength);
        }

        static TestReadResultEntry cache(long streamSegmentOffset, int requestedReadLength) {
            return new TestReadResultEntry(streamSegmentOffset, requestedReadLength, ReadResultEntryType.Cache);
        }

        static TestReadResultEntry endOfSegment(long streamSegmentOffset, int requestedReadLength) {
            return new TestReadResultEntry(streamSegmentOffset, requestedReadLength, ReadResultEntryType.EndOfStreamSegment);
        }

        static TestReadResultEntry truncated(long streamSegmentOffset, int requestedReadLength) {
            return new TestReadResultEntry(streamSegmentOffset, requestedReadLength, ReadResultEntryType.Truncated);
        }
    }

    //endregion
}
