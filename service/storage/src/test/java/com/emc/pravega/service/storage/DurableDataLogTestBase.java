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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Base class for all tests for implementations of DurableDataLog.
 */
public abstract class DurableDataLogTestBase extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    //region General DurableDataLog Tests

    /**
     * Tests the ability to append to a DurableDataLog.
     *
     * @throws Exception If one got thrown.
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
                LogAddress address = log.append(new ByteArrayInputStream(String.format("Write_%s", i).getBytes()), TIMEOUT).join();
                Assert.assertNotNull("No address returned from append().", address);
                if (prevAddress != null) {
                    AssertExtensions.assertGreaterThan("Sequence Number is not monotonically increasing.", prevAddress.getSequence(), address.getSequence());
                }

                prevAddress = address;
            }
        }
    }

    /**
     * Tests the ability to read from a DurableDataLog.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testRead() throws Exception {
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
                verifyReads(log, address, writeData);
            }

            // Test reading from a sequence number before the first one.
            verifyReads(log, createLogAddress(writeData.firstKey().getSequence() - 1), writeData);

            // Test reading from a sequence number way beyond the last one.
            verifyReads(log, createLogAddress(writeData.lastKey().getSequence() * 2), writeData);
        }
    }

    /**
     * Tests the ability to truncate from a DurableDataLog.
     *
     * @throws Exception If one got thrown.
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
                verifyReads(log, createLogAddress(-1), writeData);
            }
        }
    }

    /**
     * Tests the ability to iterate through the entries at the same time as we are modifying the list.
     *
     * @throws Exception If one got thrown.
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

    /**
     * Tests the ability to reuse the client after closing and reopening it. Verifies the major operations (append,
     * truncate and read), each operation using a different client (each client is used for exactly one operation).
     *
     * @throws Exception If one got thrown.
     */
    @Test
    @Ignore
    public void testOpenCloseClient() throws Exception {
        // This is a very repetitive test; and we only care about "recovery" from no client; all else is already tested.
        final int writeCount = 10;

        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        Object context = createSharedContext();
        LogAddress previousAddress = null;
        for (int i = 0; i < writeCount; i++) {
            // Write one entry at each iteration.
            LogAddress currentAddress;
            try (DurableDataLog log = createDurableDataLog(context)) {
                log.initialize(TIMEOUT);
                byte[] writeData = String.format("Write_%s", i).getBytes();
                currentAddress = log.append(new ByteArrayInputStream(writeData), TIMEOUT).join();
                writtenData.put(currentAddress, writeData);
            }

            // Truncate up to the previous iteration's entry, if any.
            if (previousAddress != null) {
                try (DurableDataLog log = createDurableDataLog(context)) {
                    log.initialize(TIMEOUT);
                    boolean truncated = log.truncate(previousAddress, TIMEOUT).join();
                    Assert.assertTrue("No truncation happened.", truncated);
                    writtenData.headMap(previousAddress, true).clear();
                }
            }

            // Verify reads.
            try (DurableDataLog log = createDurableDataLog(context)) {
                log.initialize(TIMEOUT);
                verifyReads(log, createLogAddress(-1), writtenData);
            }

            previousAddress = currentAddress;
        }
    }

    //endregion

    //region Implementation-specific tests

    /**
     * Tests the ability of the DurableDataLog to enforce an exclusive writer, by only allowing one client at a time
     * to write to the same physical log.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public abstract void testExclusiveWriteLock() throws Exception;

    //endregion

    //region Abstract methods.

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    protected abstract DurableDataLog createDurableDataLog();

    protected abstract DurableDataLog createDurableDataLog(Object sharedContext);

    protected abstract Object createSharedContext();

    /**
     * Creates a new LogAddress object with the given Sequence Number.
     *
     * @param seqNo Sequence Number to create LogAddress with.
     */
    protected abstract LogAddress createLogAddress(long seqNo);

    /**
     * Gets a value indicating how many writes to perform for a Write test.
     */
    protected abstract int getWriteCountForWrites();

    /**
     * Gets a value indicating how many writes to perform for a Read test.
     */
    protected abstract int getWriteCountForReads();

    //endregion

    //region Helpers

    protected TreeMap<LogAddress, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = String.format("Write_%s", i).getBytes();
            LogAddress address = log.append(new ByteArrayInputStream(writeData), TIMEOUT).join();
            writtenData.put(address, writeData);
        }

        return writtenData;
    }

    protected void verifyReads(DurableDataLog log, LogAddress afterAddress, TreeMap<LogAddress, byte[]> writeData) throws Exception {
        @Cleanup
        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader(afterAddress.getSequence());
        SortedMap<LogAddress, byte[]> expectedData = writeData.tailMap(afterAddress, false);
        Iterator<Map.Entry<LogAddress, byte[]>> expectedIterator = expectedData.entrySet().iterator();
        while (true) {
            DurableDataLog.ReadItem nextItem = reader.getNext();
            if (nextItem == null) {
                Assert.assertFalse("Reader reached the end but there were still items to be read.", expectedIterator.hasNext());
                break;
            }

            Assert.assertTrue("Reader has more items but there should not be any more items to be read.", expectedIterator.hasNext());

            // Verify sequence number, as well as payload.
            val expected = expectedIterator.next();
            Assert.assertEquals("Unexpected sequence number.", expected.getKey().getSequence(), nextItem.getAddress().getSequence());
            Assert.assertArrayEquals("Unexpected payload for sequence number " + expected.getKey(), expected.getValue(), nextItem.getPayload());
        }
    }

    //endregion
}
