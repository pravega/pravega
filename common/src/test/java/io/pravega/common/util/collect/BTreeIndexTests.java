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
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    public void testInsertSequentialNoSplit() {
        final int count = MAX_PAGE_SIZE / (KEY_LENGTH + VALUE_LENGTH) - 2;
        testInsert(count, false, false);
    }

    /**
     * Tests the insert() method using bulk-loading making sure we do not split the root page.
     */
    @Test
    public void testInsertBulkNoSplit() {
        final int count = MAX_PAGE_SIZE / (KEY_LENGTH + VALUE_LENGTH) - 2;
        // TODO: this fails. Fix first.
        testInsert(count, false, true);
    }

    /**
     * Tests the insert() method sequentially using already sorted entries.
     */
    @Test
    public void testInsertSequentialSorted() {
        // TODO: these all fail.
        testInsert(10000, true, false);
    }

    /**
     * Tests the insert() method sequentially using unsorted entries.
     */
    @Test
    public void testInsertSequentialRandom() {
        testInsert(10000, false, false);
    }

    /**
     * Tests the insert() method using bulk-loading with sorted entries.
     */
    @Test
    public void testInsertBulkSorted() {
        testInsert(10000, true, true);
    }

    /**
     * Tests the insert() method using bulk-loading with unsorted entries.
     */
    @Test
    public void testInsertBulkRandom() {
        testInsert(10000, false, true);
    }

    @Test
    public void testDelete() {

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
            for (val e : entries) {
                index.insert(Collections.singleton(e), TIMEOUT).join();
            }
        }

        // Verify index.
        check("after insert", index, entries);

        // Verify index after a full recovery.
        val recoveredIndex = defaultBuilder(ds).build();
        check("after recovery", recoveredIndex, entries);
    }

    private void check(String message, BTreeIndex index, List<PageEntry> entries){
        for(val e: entries){
            val value = index.get(e.getKey(), TIMEOUT).join();
            assertEquals(message+": value mismatch", e.getValue(), value);
        }

        // TODO: once listKeys is implemented, verify no other keys.
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
                .read(ds::read)
                .write(ds::write)
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
