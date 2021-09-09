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
package io.pravega.segmentstore.storage;

import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for all tests for implementations of DurableDataLog.
 */
public abstract class DurableDataLogTestBase extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(20 * 1000);
    protected static final int WRITE_MIN_LENGTH = 20;
    protected static final int WRITE_MAX_LENGTH = 200;
    private final Random random = new Random(0);

    //region General DurableDataLog Tests

    /**
     * Tests the ability to append to a DurableDataLog in sequence.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = 30000)
    public void testAppendSequence() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            // Check Append pre-initialization.
            AssertExtensions.assertThrows(
                    "append() worked before initialize()",
                    () -> log.append(new CompositeByteArraySegment("h".getBytes()), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);

            // Check that we cannot append data exceeding the max limit.
            AssertExtensions.assertSuppliedFutureThrows(
                    "append() worked with buffer exceeding max size",
                    () -> log.append(new CompositeByteArraySegment(log.getWriteSettings().getMaxWriteLength() + 1), TIMEOUT),
                    ex -> ex instanceof WriteTooLongException);

            // Only verify sequence number monotonicity. We'll verify reads in its own test.
            LogAddress prevAddress = null;
            int writeCount = getWriteCount();
            for (int i = 0; i < writeCount; i++) {
                LogAddress address = log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).join();
                Assert.assertNotNull("No address returned from append().", address);
                if (prevAddress != null) {
                    AssertExtensions.assertGreaterThan("Sequence Number is not monotonically increasing.", prevAddress.getSequence(), address.getSequence());
                }

                prevAddress = address;
            }
        }
    }

    /**
     * Tests the ability to append to a DurableDataLog using parallel writes.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = 30000)
    public void testAppendParallel() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            // Only verify sequence number monotonicity. We'll verify reads in its own test.
            List<CompletableFuture<LogAddress>> appendFutures = new ArrayList<>();
            int writeCount = getWriteCount();
            for (int i = 0; i < writeCount; i++) {
                appendFutures.add(log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT));
            }

            val results = Futures.allOfWithResults(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            for (int i = 0; i < results.size(); i++) {
                LogAddress address = results.get(i);
                Assert.assertNotNull("No address returned from append() for index " + i, address);
                if (i > 0) {
                    AssertExtensions.assertGreaterThan("Sequence Number is not monotonically increasing.",
                            results.get(i - 1).getSequence(), address.getSequence());
                }
            }
        }
    }

    /**
     * Tests the ability to read from a DurableDataLog.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = 30000)
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
     * Tests the ability to execute reads after recovery, immediately followed by writes.
     *
     * @throws Exception If one got thrown.
     */
    @Test(timeout = 30000)
    public void testReadWriteRecovery() throws Exception {
        final int iterationCount = 4;
        Object context = createSharedContext();
        TreeMap<LogAddress, byte[]> writeData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        for (int i = 0; i < iterationCount; i++) {
            try (DurableDataLog log = createDurableDataLog(context)) {
                log.initialize(TIMEOUT);
                verifyReads(log, writeData);
                writeData.putAll(populate(log, getWriteCount()));
            }
        }

        // One last recovery at the end.
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
    @Test(timeout = 30000)
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
            writeData = populate(log, getTruncateWriteCount());
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
    @Test(timeout = 30000)
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
            log.append(new CompositeByteArraySegment("foo".getBytes()), TIMEOUT).join();

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
    @Test(timeout = 30000)
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
                currentAddress = log.append(new CompositeByteArraySegment(writeData), TIMEOUT).join();
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
    @Test(timeout = 30000)
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
                AssertExtensions.assertSuppliedFutureThrows(
                        "The first log was not fenced out.",
                        () -> log1.append(new CompositeByteArraySegment(new byte[1]), TIMEOUT),
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

    /**
     * Verifies the ability to Enable or Disable Logs.
     *
     * @throws Exception If one occurred.
     */
    @Test(timeout = 30000)
    public void testEnableDisable() throws Exception {
        Object context = createSharedContext();
        try (DurableDataLog log = createDurableDataLog(context)) {
            // Disable should not work without the lock.
            AssertExtensions.assertThrows(
                    "disable() worked when the log not initialized.",
                    log::disable,
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);

            // Enable should not work if it's already enabled.
            AssertExtensions.assertThrows(
                    "enable() worked when the log was already initialized/enabled.",
                    log::enable,
                    ex -> ex instanceof IllegalStateException);

            // Disable the log.
            log.disable();

            AssertExtensions.assertThrows(
                    "disable() did not close the Log.",
                    () -> log.append(new CompositeByteArraySegment(1), TIMEOUT),
                    ex -> ex instanceof ObjectClosedException);
        }

        TreeMap<LogAddress, byte[]> writeData;
        try (DurableDataLog log1 = createDurableDataLog(context)) {
            // By contract, initialization (lock acquisition) should not succeed on a disabled log.
            AssertExtensions.assertThrows(
                    "initialize worked with a disabled log.",
                    () -> log1.initialize(TIMEOUT),
                    ex -> ex instanceof DataLogDisabledException);

            // Verify that Log operations cannot execute while disabled.
            AssertExtensions.assertThrows(
                    "append() worked with a disabled/non-initialized log.",
                    () -> log1.append(new CompositeByteArraySegment(1), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);
            AssertExtensions.assertThrows(
                    "truncate() worked with a disabled/non-initialized log.",
                    () -> log1.truncate(createLogAddress(1), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);
            AssertExtensions.assertThrows(
                    "getReader() worked with a disabled/non-initialized log.",
                    log1::getReader,
                    ex -> ex instanceof IllegalStateException);

            // Open up a second log and verify that the "disabled" flag was properly persisted. Then enable the log using
            // this second long.
            try (DurableDataLog log2 = createDurableDataLog(context)) {
                AssertExtensions.assertThrows(
                        "initialize worked with a disabled log (second instance).",
                        () -> log2.initialize(TIMEOUT),
                        ex -> ex instanceof DataLogDisabledException);

                log2.enable();
                log2.initialize(TIMEOUT);
            }

            // Now verify that initial log is able to initialize again and resume normal operations.
            log1.initialize(TIMEOUT);
            writeData = populate(log1, getWriteCount());
        }

        // Simulate Container recovery: we always read only upon recovery; never while writing.
        try (DurableDataLog log3 = createDurableDataLog(context)) {
            log3.initialize(TIMEOUT);
            verifyReads(log3, writeData);
        }
    }

    /**
     * Tests the ability to register a {@link ThrottleSourceListener} and notify it of updates.
     * @throws Exception If an error occurred.
     */
    @Test(timeout = 30000)
    public void testRegisterQueueStateListener() throws Exception {
        val listener = new TestThrottleSourceListener();
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            log.registerQueueStateChangeListener(listener);

            // Only verify sequence number monotonicity. We'll verify reads in its own test.
            int writeCount = getWriteCount();
            for (int i = 0; i < writeCount; i++) {
                log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).join();
            }

            // Verify the correct number of invocations.
            TestUtils.await(
                    () -> listener.getCount() == writeCount,
                    10,
                    TIMEOUT.toMillis());

            // Verify that the listener is unregistered when closed.
            listener.close();
            log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).join();
            try {
                TestUtils.await(
                        () -> listener.getCount() > writeCount,
                        10,
                        50);
                Assert.fail("Listener's count was updated after it was closed.");
            } catch (TimeoutException tex) {
                // This is expected. We do not want our condition to hold true.
            }
            log.registerQueueStateChangeListener(listener); // This should have no effect as it's already closed.
            Assert.assertFalse("Not expected the listener to have been notified after closing.", listener.wasNotifiedWhenClosed());
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

    /**
     * Gets a value indicating how many writes to perform in a truncate test..
     */
    protected int getTruncateWriteCount() {
        return getWriteCount() / 4;
    }

    //endregion

    //region Helpers

    protected byte[] getWriteData() {
        int length = WRITE_MIN_LENGTH + random.nextInt(WRITE_MAX_LENGTH - WRITE_MIN_LENGTH);
        byte[] data = new byte[length];
        this.random.nextBytes(data);
        return data;
    }

    protected TreeMap<LogAddress, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<LogAddress, byte[]> writtenData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        val data = new ArrayList<byte[]>();
        val futures = new ArrayList<CompletableFuture<LogAddress>>();
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = getWriteData();
            futures.add(log.append(new CompositeByteArraySegment(writeData), TIMEOUT));
            data.add(writeData);
        }

        val addresses = Futures.allOfWithResults(futures).join();
        for (int i = 0; i < data.size(); i++) {
            writtenData.put(addresses.get(i), data.get(i));
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

    //region TestThrottleSourceListener

    private static class TestThrottleSourceListener implements ThrottleSourceListener {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger count = new AtomicInteger();
        private final AtomicBoolean notifyWhenClosed = new AtomicBoolean(false);

        void close() {
            this.closed.set(true);
        }

        boolean wasNotifiedWhenClosed() {
            return this.notifyWhenClosed.get();
        }

        int getCount() {
            return this.count.get();
        }

        @Override
        public void notifyThrottleSourceChanged() {
            if (this.closed.get()) {
                this.notifyWhenClosed.set(true);
            }

            Exceptions.checkNotClosed(this.closed.get(), this);
            this.count.incrementAndGet();
        }

        @Override
        public boolean isClosed() {
            return this.closed.get();
        }
    }

    //endregion
}
