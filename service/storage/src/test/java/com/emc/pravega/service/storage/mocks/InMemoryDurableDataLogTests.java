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

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Unit tests for InMemoryDurableDataLog.
 */
public class InMemoryDurableDataLogTests {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int WRITE_COUNT = 250;

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
            for (int i = 0; i < WRITE_COUNT; i++) {
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
            TreeMap<Long, byte[]> writeData = populate(log, WRITE_COUNT);

            // Test reading after each sequence number that we got back.
            for (long seqNo : writeData.keySet()) {
                testRead(log, seqNo, writeData);
            }

            // Test reading from a sequence number before the first one.
            testRead(log, writeData.firstKey() - 1, writeData);

            // Test reading from a sequence number way beyond the last one.
            testRead(log, writeData.lastKey() * 2, writeData);
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
                    () -> log.truncate(new InMemoryDurableDataLog.InMemoryLogAddress(0), TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            log.initialize(TIMEOUT);
            TreeMap<Long, byte[]> writeData = populate(log, WRITE_COUNT);
            ArrayList<Long> seqNos = new ArrayList<>(writeData.keySet());

            // Test truncating after each sequence number that we got back.
            for (long seqNo : seqNos) {
                log.truncate(new InMemoryDurableDataLog.InMemoryLogAddress(seqNo), TIMEOUT).join();
                writeData.remove(seqNo);
                testRead(log, -1, writeData);
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
            populate(log, WRITE_COUNT);

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
     * Tests the ability of InMemoryDurableDataLog to simulate an exclusive writer, by only allowing one client at a time
     * to write to an "EntryCollection".
     */
    @Test
    public void testExclusiveWriteLock() throws Exception {
        InMemoryDurableDataLog.EntryCollection entries = new InMemoryDurableDataLog.EntryCollection();

        try (DurableDataLog log = new InMemoryDurableDataLog(entries)) {
            log.initialize(TIMEOUT);

            // 1. No two logs can use the same EntryCollection.
            AssertExtensions.assertThrows(
                    "A second log was able to acquire the exclusive write lock, even if another log held it.",
                    () -> {
                        try (DurableDataLog log2 = new InMemoryDurableDataLog(entries)) {
                            log2.initialize(TIMEOUT);
                        }
                    },
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // Verify we can still append to the first log.
            TreeMap<Long, byte[]> writeData = populate(log, WRITE_COUNT);

            // 2. If during the normal operation of a log, it loses its lock, it should no longer be able to append...
            entries.forceAcquireLock("ForceLock");
            AssertExtensions.assertThrows(
                    "A second log was able to acquire the exclusive write lock, even if another log held it.",
                    () -> log.append(new ByteArrayInputStream("h".getBytes()), TIMEOUT).join(),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // ... or to truncate ...
            AssertExtensions.assertThrows(
                    "A second log was able to acquire the exclusive write lock, even if another log held it.",
                    () -> log.truncate(new InMemoryDurableDataLog.InMemoryLogAddress(writeData.lastKey()), TIMEOUT).join(),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // ... but it should still be able to read.
            testRead(log, -1, writeData);
        }
    }

    /**
     * Tests the constructor of InMemoryDurableDataLog. The constructor takes in an EntryCollection and this verifies
     * that information from a previous instance of an InMemoryDurableDataLog is still accessible.
     */
    @Test
    public void testConstructor() throws Exception {
        InMemoryDurableDataLog.EntryCollection entries = new InMemoryDurableDataLog.EntryCollection();
        TreeMap<Long, byte[]> writeData;
        // Create first log and write some data to it.
        try (DurableDataLog log = new InMemoryDurableDataLog(entries)) {
            log.initialize(TIMEOUT);
            writeData = populate(log, WRITE_COUNT);
        }

        // Close the first log, and open a second one, with the same EntryCollection in the constructor.
        try (DurableDataLog log = new InMemoryDurableDataLog(entries)) {
            log.initialize(TIMEOUT);

            // Verify it contains the same entries.
            testRead(log, -1, writeData);
        }
    }

    //endregion

    private TreeMap<Long, byte[]> populate(DurableDataLog log, int writeCount) {
        TreeMap<Long, byte[]> writtenData = new TreeMap<>();
        for (int i = 0; i < writeCount; i++) {
            byte[] writeData = String.format("Write_%s", i).getBytes();
            ByteArrayInputStream writeStream = new ByteArrayInputStream(writeData);
            LogAddress address = log.append(writeStream, TIMEOUT).join();
            writtenData.put(address.getSequence(), writeData);
        }

        return writtenData;
    }

    private void testRead(DurableDataLog log, long afterSequenceNumber, TreeMap<Long, byte[]> writeData) throws Exception {
        @Cleanup
        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader = log.getReader(afterSequenceNumber);
        SortedMap<Long, byte[]> expectedData = writeData.tailMap(afterSequenceNumber, false);
        Iterator<Long> expectedKeyIterator = expectedData.keySet().iterator();
        while (true) {
            DurableDataLog.ReadItem nextItem = reader.getNext();
            if (nextItem == null) {
                Assert.assertFalse("Reader reached the end but there were still items to be read.", expectedKeyIterator.hasNext());
                break;
            }

            Assert.assertTrue("Reader has more items but there should not be any more items to be read.", expectedKeyIterator.hasNext());

            // Verify sequence number, as well as payload.
            long expectedSequenceNumber = expectedKeyIterator.next();
            Assert.assertEquals("Unexpected sequence number.", expectedSequenceNumber, nextItem.getAddress().getSequence());
            Assert.assertArrayEquals("Unexpected payload for sequence number " + expectedSequenceNumber, expectedData.get(expectedSequenceNumber), nextItem.getPayload());
        }
    }

    private DurableDataLog createDurableDataLog() {
        return new InMemoryDurableDataLog(new InMemoryDurableDataLog.EntryCollection());
    }
}
