/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableBucketReader} class.
 */
public class TableBucketReaderTests extends ThreadPooledTestSuite {
    private static final int COUNT = 10;
    private static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 16;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Tests the ability to locate Table Keys in a Table Bucket using {@link TableBucketReader#key}.
     */
    @Test
    public void testFindKey() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val data = generateData();
        segment.append(data.serialization, null, TIMEOUT).join();
        val reader = TableBucketReader.key(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(data.getBackpointer(offset)),
                executorService());

        // Check a valid result.
        val validKey = data.entries.get(1).getKey();
        val validResult = reader.find(validKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected version from valid key.", data.getEntryOffset(1), validResult.getVersion());
        Assert.assertTrue("Unexpected 'valid' key returned.", HashedArray.arrayEquals(validKey.getKey(), validResult.getKey()));

        // Check a key that does not exist.
        val invalidKey = data.unlinkedEntry.getKey();
        val invalidResult = reader.find(invalidKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that does not exist.", invalidResult);
    }

    /**
     * Tests the ability to locate Table Entries in a Table Bucket using {@link TableBucketReader#key}.
     */
    @Test
    public void testFindEntry() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val data = generateData();
        segment.append(data.serialization, null, TIMEOUT).join();
        val reader = TableBucketReader.entry(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(data.getBackpointer(offset)),
                executorService());

        // Check a valid result.
        val validEntry = data.entries.get(1);
        val validResult = reader.find(validEntry.getKey().getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected version from valid key.", data.getEntryOffset(1), validResult.getKey().getVersion());
        Assert.assertTrue("Unexpected 'valid' key returned.", HashedArray.arrayEquals(validEntry.getKey().getKey(), validResult.getKey().getKey()));
        Assert.assertTrue("Unexpected 'valid' key returned.", HashedArray.arrayEquals(validEntry.getValue(), validResult.getValue()));

        // Check a key that does not exist.
        val invalidKey = data.unlinkedEntry.getKey();
        val invalidResult = reader.find(invalidKey.getKey(), data.getBucketOffset(), new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that does not exist.", invalidResult);
    }

    /**
     * Tests the ability to (not) locate Table Entries in a Table Bucket for deleted keys.
     */
    @Test
    public void testFindEntryDeleted() throws Exception {
        val segment = new SegmentMock(executorService());

        // Generate our test data and append it to the segment.
        val deletedKey = generateEntries().get(0).getKey();
        val es = new EntrySerializer();
        byte[] data = new byte[es.getRemovalLength(deletedKey)];
        es.serializeRemoval(Collections.singleton(deletedKey), data);
        segment.append(data, null, TIMEOUT).join();
        val reader = TableBucketReader.entry(segment,
                (s, offset, timeout) -> CompletableFuture.completedFuture(-1L), // No backpointers.
                executorService());

        val deletedResult = reader.find(deletedKey.getKey(), 0L, new TimeoutTimer(TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any result for key that was deleted.", deletedResult);
    }

    private TestData generateData() {
        val entries = generateEntries();
        val s = new EntrySerializer();
        val length = entries.stream().mapToInt(s::getUpdateLength).sum();
        byte[] serialization = new byte[length];
        s.serializeUpdate(entries, serialization);
        int entryLength = length / entries.size(); // All entries have the same length.
        val backpointers = new HashMap<Long, Long>();

        // The first entry is not linked; we use that to search for inexistent keys.
        for (int i = 2; i < entries.size(); i++) {
            backpointers.put((long) i * entryLength, (long) (i - 1) * entryLength);
        }

        return new TestData(entries, serialization, backpointers, entries.get(0), entryLength);
    }

    private List<TableEntry> generateEntries() {
        val rnd = new Random(0);
        val result = new ArrayList<TableEntry>();
        for (int i = 0; i < COUNT; i++) {
            byte[] keyData = new byte[KEY_LENGTH];
            rnd.nextBytes(keyData);
            byte[] valueData = new byte[VALUE_LENGTH];
            rnd.nextBytes(valueData);
            result.add(TableEntry.unversioned(new ByteArraySegment(keyData), new ByteArraySegment(valueData)));
        }

        return result;
    }

    @RequiredArgsConstructor
    private static class TestData {
        final List<TableEntry> entries;
        final byte[] serialization;
        final Map<Long, Long> backpointers;
        final TableEntry unlinkedEntry;
        final int entryLength;

        long getEntryOffset(int index) {
            return (long) index * entryLength;
        }

        long getBucketOffset() {
            return getEntryOffset(this.entries.size() - 1);
        }

        long getBackpointer(long source) {
            return this.backpointers.getOrDefault(source, -1L);
        }
    }
}
