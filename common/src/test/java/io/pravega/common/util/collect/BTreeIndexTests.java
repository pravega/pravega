/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import com.google.common.base.Preconditions;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the BTreeIndex class.
 */
public class BTreeIndexTests extends ThreadPooledTestSuite {
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 8;
    private static final int MAX_PAGE_SIZE = 1024;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the insert() method sequentially making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitSequential() {
        final int count = MAX_PAGE_SIZE / (KEY_LENGTH + VALUE_LENGTH) - 2;
        testInsert(count, false, false);
    }

    /**
     * Tests the insert() method using bulk-loading making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitBulk() {
        final int count = MAX_PAGE_SIZE / (KEY_LENGTH + VALUE_LENGTH) - 2;
        testInsert(count, false, true);
    }

    /**
     * Tests the insert() method sequentially using already sorted entries.
     */
    @Test
    public void testInsertSortedSequential() {
        testInsert(10000, true, false);
    }

    /**
     * Tests the insert() method sequentially using unsorted entries.
     */
    @Test
    public void testInsertRandomSequential() {
        testInsert(10000, false, false);
    }

    /**
     * Tests the insert() method using bulk-loading with sorted entries.
     */
    @Test
    public void testInsertSortedBulk() {
        testInsert(10000, true, true);
    }

    /**
     * Tests the insert() method using bulk-loading with unsorted entries.
     */
    @Test
    public void testInsertRandomBulk() {
        testInsert(10000, false, true);
    }

    /**
     * Tests the delete() method sequentially.
     */
    @Test
    public void testDeleteSequential() {
        testDelete(100, 1);
    }

    /**
     * Tests the delete() method using multiple keys at once..
     */
    @Test
    public void testDeleteBulk() {
        testDelete(1000, 499);
    }

    /**
     * Tests the delete() method for all the keys at once.
     */
    @Test
    public void testDeleteAll() {
        final int count = 10000;
        testDelete(count, count);
    }

    @Test
    public void testConcurrentModification() {

    }

    @Test
    public void testGet() {

    }

    @Test
    public void testGetBulk() {

    }

    private void testDelete(int count, int deleteBatchSize) {
        final int checkEvery = count / 10; // checking is very expensive; we don't want to do it every time.
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        val entries = generate(count);
        long lastRetVal = index.insert(entries, TIMEOUT).join();

        int firstIndex = 0;
        int lastCheck = -1;
        while (firstIndex < count) {
            int batchSize = Math.min(deleteBatchSize, count - firstIndex);
            val toDelete = entries.subList(firstIndex, firstIndex + batchSize)
                    .stream().map(PageEntry::getKey).collect(Collectors.toList());
            val remainingEntries = entries.subList(firstIndex + batchSize, entries.size());
            long retVal = index.delete(toDelete, TIMEOUT).join();
            AssertExtensions.assertGreaterThan("Expecting return value to increase.", lastRetVal, retVal);

            // TODO: also try to fetch ALL
            // Determine if it's time to check the index.
            if (firstIndex - lastCheck > checkEvery) {
                check("after deleting " + (firstIndex + 1), index, remainingEntries);
                lastCheck = firstIndex;
            }

            firstIndex += batchSize;
        }

        check("at the end", index, Collections.emptyList());
    }

    private void testInsert(int count, boolean sorted, boolean bulk) {
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        val entries = generate(count);
        if (sorted) {
            sort(entries);
        }

        if (bulk) {
            index.insert(entries, TIMEOUT).join();
        } else {
            long lastRetVal = 0;
            for (val e : entries) {
                long retVal = index.insert(Collections.singleton(e), TIMEOUT).join();
                AssertExtensions.assertGreaterThan("Expecting return value to increase.", lastRetVal, retVal);
                lastRetVal = retVal;
            }
        }

        // Verify index.
        check("after insert", index, entries);

        // Verify index after a full recovery.
        val recoveredIndex = defaultBuilder(ds).build();
        check("after recovery", recoveredIndex, entries);
    }

    private void check(String message, BTreeIndex index, List<PageEntry> entries){
        // Use bulk-get since it's faster.
        val keys = entries.stream().map(PageEntry::getKey).collect(Collectors.toList());
        val actualValues = index.get(keys, TIMEOUT).join();

        // Bulk-get returns a list of values in the same order as the keys, so we need to match up on the indices.
        Assert.assertEquals("Unexpected key count.", keys.size(), actualValues.size());
        for (int i = 0; i < keys.size(); i++) {
            val av = actualValues.get(i);
            val expectedValue = entries.get(i).getValue();
            assertEquals(message + ": value mismatch for entry index " + i, expectedValue, av);
        }

        // TODO: once iterateKeys is implemented, verify no other keys.
    }

    private ArrayList<PageEntry> generate(int count) {
        val result = new ArrayList<PageEntry>(count);
        val rnd = new Random(count);
        for (int i = 0; i < count; i++) {
            val key = new byte[KEY_LENGTH];
            val value = new byte[VALUE_LENGTH];
            rnd.nextBytes(key);
            rnd.nextBytes(value);
            result.add(new PageEntry(new ByteArraySegment(key), new ByteArraySegment(value)));
        }

        return result;
    }

    private void sort(List<PageEntry> entries) {
        entries.sort((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()));
    }

    private BTreeIndex.BTreeIndexBuilder defaultBuilder(DataSource ds) {
        return BTreeIndex.builder()
                .maxPageSize(MAX_PAGE_SIZE)
                .keyLength(KEY_LENGTH)
                .valueLength(VALUE_LENGTH)
                .readPage(ds::read)
                .writePages(ds::write)
                .getLength(ds::getLength)
                .executor(executorService());
    }

    private void assertEquals(String message, ByteArraySegment b1, ByteArraySegment b2) {
        if (b1.getLength() != b2.getLength() || KEY_COMPARATOR.compare(b1, b2) != 0) {
            Assert.fail(message);
        }
    }

    @ThreadSafe
    private class DataSource {
        @GuardedBy("data")
        private final EnhancedByteArrayOutputStream data;

        DataSource() {
            this.data = new EnhancedByteArrayOutputStream();
        }

        CompletableFuture<Long> getLength(Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    return (long) this.data.size();
                }
            }, executorService());
        }

        CompletableFuture<ByteArraySegment> read(long offset, int length, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    return new ByteArraySegment(this.data.getData().subSegment((int) offset, length).getCopy());

                }
            }, executorService());
        }

        CompletableFuture<Void> write(long expectedOffset, InputStream toWrite, int length, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.data) {
                    Preconditions.checkArgument(expectedOffset == this.data.size(), "bad offset");
                    try {
                        byte[] buffer = new byte[1024];
                        int totalBytesCopied = 0;
                        while (totalBytesCopied < length) {
                            int copied = toWrite.read(buffer);
                            if (copied > 0) {
                                this.data.write(buffer, 0, copied);
                            } else {
                                break;
                            }
                            totalBytesCopied += copied;
                        }
                        Preconditions.checkArgument(totalBytesCopied == length, "not enough data to copy");
                    } catch (Exception ex) {
                        throw new CompletionException(ex);
                    }
                }
            }, executorService());
        }

    }
}
