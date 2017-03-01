/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage;

import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for all tests for implementations of DurableDataLog.
 */
public abstract class DurableDataLogTestBase extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    //region General DurableDataLog Tests

    /**
     * Tests the ability to append to a DurableDataLog.
     */
    @Test
    public void testAppend() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            // Check Append pre-initialization.
            AssertExtensions.assertThrows(
                    "append() worked before initialize()",
                    () -> log.append(new ByteArrayInputStream("h".getBytes()), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);

            // Only verify sequence number monotonicity. We'll verify reads in its own test.
            LogAddress prevAddress = null;
            int writeCount = getWriteCountForWrites();
            for (int i = 0; i < writeCount; i++) {
                byte[] writeData = String.format("Write_%s", i).getBytes();
                ByteArrayInputStream writeStream = new ByteArrayInputStream(writeData);
                LogAddress address = log.append(writeStream, TIMEOUT).join();

                if (prevAddress != null) {
                    AssertExtensions.assertGreaterThan("Sequence Number is not monotonically increasing.", prevAddress.getSequence(), address.getSequence());
                }

                prevAddress = address;
            }
        }
    }

    /**
     * Tests the ability to read from a DurableDataLog.
     */
    @Test
    public void testGetReader() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            // Check Read pre-initialization.
            AssertExtensions.assertThrows(
                    "read() worked before initialize()",
                    () -> log.getReader(0),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);
            val writeData = populate(log, getWriteCountForReads());

            // Test reading after each sequence number that we got back.
            for (LogAddress address : writeData.keySet()) {
                testRead(log, address, writeData);
            }

            // Test reading from a sequence number before the first one.
            testRead(log, createLogAddress(writeData.firstKey().getSequence() - 1), writeData);

            // Test reading from a sequence number way beyond the last one.
            testRead(log, createLogAddress(writeData.lastKey().getSequence() * 2), writeData);
        }
    }

    /**
     * Tests the ability to truncate from a DurableDataLog.
     */
    @Test
    public void testTruncate() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            // Check Read pre-initialization.
            AssertExtensions.assertThrows(
                    "truncate() worked before initialize()",
                    () -> log.truncate(createLogAddress(0), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);
            TreeMap<LogAddress, byte[]> writeData = populate(log, getWriteCountForReads());
            ArrayList<LogAddress> addresses = new ArrayList<>(writeData.keySet());

            // Test truncating after each sequence number that we got back.
            for (LogAddress address : addresses) {
                boolean truncated = log.truncate(address, TIMEOUT).join();
                Assert.assertTrue("No truncation happened.", truncated);
                writeData.headMap(address, true).clear();
                testRead(log, createLogAddress(-1), writeData);
            }
        }
    }

    /**
     * Tests the ability to iterate through the entries at the same time as we are modifying the list.
     */
    @Test
    public void testConcurrentIterator() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            populate(log, getWriteCountForWrites());

            // Create a reader and read one item.
            @Cleanup
            CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader(-1);
            DurableDataLog.ReadItem firstItem = reader.getNext();
            Assert.assertNotNull("Nothing read before modification.", firstItem);

            // Make a modification.
            log.append(new ByteArrayInputStream("foo".getBytes()), TIMEOUT).join();

            // Try to get a new item.
            DurableDataLog.ReadItem secondItem = reader.getNext();
            Assert.assertNotNull("Nothing read after modification.", secondItem);
        }
    }

    //endregion

    //region InMemoryDurableDataLog-specific tests

    /**
     * Tests the ability of the DurableDataLog to enforce an exclusive writer, by only allowing one client at a time
     * to write to the same physical log.
     */
    @Test
    public abstract void testExclusiveWriteLock() throws Exception;

    //endregion

    protected TreeMap<LogAddress, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = String.format("Write_%s", i).getBytes();
            ByteArrayInputStream writeStream = new ByteArrayInputStream(writeData);
            LogAddress address = log.append(writeStream, TIMEOUT).join();
            writtenData.put(address, writeData);
        }

        return writtenData;
    }

    protected void testRead(DurableDataLog log, LogAddress afterAddress, TreeMap<LogAddress, byte[]> writeData) throws Exception {
        @Cleanup
        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader(afterAddress.getSequence());
        SortedMap<LogAddress, byte[]> expectedData = writeData.tailMap(afterAddress, false);
        Iterator<LogAddress> expectedKeyIterator = expectedData.keySet().iterator();
        while (true) {
            DurableDataLog.ReadItem nextItem = reader.getNext();
            if (nextItem == null) {
                Assert.assertFalse("Reader reached the end but there were still items to be read.", expectedKeyIterator.hasNext());
                break;
            }

            Assert.assertTrue("Reader has more items but there should not be any more items to be read.", expectedKeyIterator.hasNext());

            // Verify sequence number, as well as payload.
            LogAddress expectedAddress = expectedKeyIterator.next();
            Assert.assertEquals("Unexpected sequence number.", expectedAddress.getSequence(), nextItem.getAddress().getSequence());
            Assert.assertArrayEquals("Unexpected payload for sequence number " + expectedAddress, expectedData.get(expectedAddress), nextItem.getPayload());
        }
    }

    //region Abstract methods

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    protected abstract DurableDataLog createDurableDataLog();

    protected abstract LogAddress createLogAddress(long seqNo);

    protected abstract int getWriteCountForWrites();

    protected abstract int getWriteCountForReads();

    //endregion
}
