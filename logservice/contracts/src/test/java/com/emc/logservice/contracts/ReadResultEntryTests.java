package com.emc.logservice.contracts;

import com.emc.logservice.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

/**
 * Unit tests for ReadResultEntry class.
 */
public class ReadResultEntryTests {
    /**
     * Tests the ability of the ReadResultEntry base class to adjust offsets when instructed so
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

    private static class TestReadResultEntry extends ReadResultEntry {
        public TestReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
            super(streamSegmentOffset, requestedReadLength);
        }

        @Override
        public CompletableFuture<ReadResultEntryContents> getContent() {
            return null;
        }
    }
}
