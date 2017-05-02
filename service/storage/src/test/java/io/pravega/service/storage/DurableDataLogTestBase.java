/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage;

import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for all tests for implementations of DurableDataLog.
 */
public abstract class DurableDataLogTestBase extends ThreadPooledTestSuite {
    protected static final int TIMEOUT_MILLIS = 60 * 1000;
    protected static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MILLIS);
    protected static final int WRITE_MIN_LENGTH = 20;
    protected static final int WRITE_MAX_LENGTH = 200;
    private final Random random = new Random(0);

    //region General DurableDataLog Tests

    /**
     * Tests the ability to append to a DurableDataLog.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = TIMEOUT_MILLIS)
    public void testAppend() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            // Check Append pre-initialization.
            AssertExtensions.assertThrows(
                    "append() worked before initialize()",
                    () -> log.append(new ByteArraySegment("h".getBytes()), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);

            // Only verify sequence number monotonicity. We'll verify reads in its own test.
            LogAddress prevAddress = null;
            int writeCount = getWriteCount();
            for (int i = 0; i < writeCount; i++) {
                LogAddress address = log.append(new ByteArraySegment(getWriteData()), TIMEOUT).join();
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
    @Test(timeout = TIMEOUT_MILLIS)
    public void testRead() throws Exception {
        TreeMap<LogAddress, byte[]> writeData;
        Object context = createSharedContext();
        try (DurableDataLog log = createDurableDataLog(context)) {
            // Check Read pre-initialization.
            AssertExtensions.assertThrows(
                    "read() worked before initialize()",
                    log::getReader,
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);
            writeData = populate(log, getWriteCount());
        }

        // Simulate Container recovery: we always read only upon recovery; never while writing.
        try (DurableDataLog log = createDurableDataLog(context)) {
            log.initialize(TIMEOUT);
            verifyReads(log, writeData);
        }
    }

    /**
     * Tests the ability to truncate from a DurableDataLog.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = TIMEOUT_MILLIS)
    public void testTruncate() throws Exception {
        TreeMap<LogAddress, byte[]> writeData;
        ArrayList<LogAddress> addresses;
        Object context = createSharedContext();
        try (DurableDataLog log = createDurableDataLog(context)) {
            // Check Read pre-initialization.
            AssertExtensions.assertThrows(
                    "truncate() worked before initialize()",
                    () -> log.truncate(createLogAddress(0), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);
            writeData = populate(log, getWriteCount());
            addresses = new ArrayList<>(writeData.keySet());
        }

        try (DurableDataLog log = createDurableDataLog(context)) {
            log.initialize(TIMEOUT);
            // Test truncating after each sequence number that we got back.
            for (LogAddress address : addresses) {
                log.truncate(address, TIMEOUT).join();
                writeData.headMap(address, true).clear();
                verifyReads(log, writeData);
            }
        }
    }

    /**
     * Tests the ability to iterate through the entries at the same time as we are modifying the list.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = TIMEOUT_MILLIS)
    public void testConcurrentIterator() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            populate(log, getWriteCount());

            // Create a reader and read one item.
            @Cleanup
            CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader();
            DurableDataLog.ReadItem firstItem = reader.getNext();
            Assert.assertNotNull("Nothing read before modification.", firstItem);

            // Make a modification.
            log.append(new ByteArraySegment("foo".getBytes()), TIMEOUT).join();

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
    @Test(timeout = TIMEOUT_MILLIS)
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
                currentAddress = log.append(new ByteArraySegment(writeData), TIMEOUT).join();
                writtenData.put(currentAddress, writeData);
            }

            // Truncate up to the previous iteration's entry, if any.
            if (previousAddress != null) {
                try (DurableDataLog log = createDurableDataLog(context)) {
                    log.initialize(TIMEOUT);
                    log.truncate(previousAddress, TIMEOUT).join();
                    writtenData.headMap(previousAddress, true).clear();
                }
            }

            // Verify reads.
            try (DurableDataLog log = createDurableDataLog(context)) {
                log.initialize(TIMEOUT);
                verifyReads(log, writtenData);
            }

            previousAddress = currentAddress;
        }
    }

    /**
     * Tests the ability of the DurableDataLog to enforce an exclusive writer, by only allowing one client at a time
     * to write to the same physical log.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testExclusiveWriteLock() throws Exception {
        final long initialEpoch;
        final long secondEpoch;
        TreeMap<LogAddress, byte[]> writeData;
        Object context = createSharedContext();
        try (DurableDataLog log1 = createDurableDataLog(context)) {
            log1.initialize(TIMEOUT);
            initialEpoch = log1.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", 0, initialEpoch);

            // 1. No two logs can use the same EntryCollection.
            try (DurableDataLog log2 = createDurableDataLog(context)) {
                log2.initialize(TIMEOUT);
                secondEpoch = log2.getEpoch();
                AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", initialEpoch, secondEpoch);

                // Verify we cannot write to the first log.
                AssertExtensions.assertThrows(
                        "The first log was not fenced out.",
                        () -> log1.append(new ByteArraySegment(new byte[1]), TIMEOUT),
                        ex -> ex instanceof DataLogWriterNotPrimaryException);

                // Verify we can write to the second log.
                writeData = populate(log2, getWriteCount());
            }
        }

        try (DurableDataLog log = createDurableDataLog(context)) {
            log.initialize(TIMEOUT);
            long thirdEpoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on non-empty log initialization.", secondEpoch, thirdEpoch);
            verifyReads(log, writeData);
        }
    }

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
     * Gets a value indicating how many writes to perform in any test.
     */
    protected abstract int getWriteCount();

    //endregion

    //region Helpers

    private byte[] getWriteData() {
        int length = WRITE_MIN_LENGTH + random.nextInt(WRITE_MAX_LENGTH - WRITE_MIN_LENGTH);
        byte[] data = new byte[length];
        this.random.nextBytes(data);
        return data;
    }

    protected TreeMap<LogAddress, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = getWriteData();
            LogAddress address = log.append(new ByteArraySegment(writeData), TIMEOUT).join();
            writtenData.put(address, writeData);
        }

        return writtenData;
    }

    protected void verifyReads(DurableDataLog log, TreeMap<LogAddress, byte[]> writeData) throws Exception {
        @Cleanup
        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader();
        Iterator<Map.Entry<LogAddress, byte[]>> expectedIterator = writeData.entrySet().iterator();
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
            val actualPayload = StreamHelpers.readAll(nextItem.getPayload(), nextItem.getLength());
            Assert.assertArrayEquals("Unexpected payload for sequence number " + expected.getKey(), expected.getValue(), actualPayload);
        }
    }

    //endregion
}
