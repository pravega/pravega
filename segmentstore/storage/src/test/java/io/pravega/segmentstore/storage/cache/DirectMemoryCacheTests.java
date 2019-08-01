/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.cache;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link DirectMemoryCache} class.
 */
public class DirectMemoryCacheTests {
    private static final CacheLayout LAYOUT = new CacheLayout.DefaultLayout();
    private static final long REQUESTED_MAX_SIZE = 11 * 1024 * 1024; // 1MB
    private static final long ACTUAL_MAX_SIZE = REQUESTED_MAX_SIZE - REQUESTED_MAX_SIZE % LAYOUT.bufferSize() + LAYOUT.bufferSize();
    private static final int BUFFER_COUNT = (int) (ACTUAL_MAX_SIZE / LAYOUT.bufferSize());
    private final Random rnd = new Random(0);

    /**
     * Verifies the allocation and deallocation (when closing) of direct memory.
     */
    @Test
    public void testAllocateClose() {
        final int writeSize = 1;
        long storedBytes = 0;
        long usedBytes = 0;
        long reservedBytes = 0;
        long allocatedBytes = 0;

        // Initially it should be empty.
        @Cleanup
        val c = new TestCache();
        checkSnapshot(c, storedBytes, usedBytes, reservedBytes, allocatedBytes, ACTUAL_MAX_SIZE);

        // Fill up the cache and verify, at each step, what should happen. Pre-calculate the number of writes that would
        // fill it up so we would know right away if something gets missed or overwritten.
        int writeCount = BUFFER_COUNT * (LAYOUT.blocksPerBuffer() - 1);
        int lastBufferId = -1;
        for (int i = 0; i < writeCount; i++) {
            val address = c.insert(new ByteArraySegment(new byte[writeSize]));
            int bufferId = LAYOUT.getBufferId(address);
            if (bufferId != lastBufferId) {
                lastBufferId = bufferId;
                allocatedBytes += LAYOUT.bufferSize();
                reservedBytes += LAYOUT.blockSize(); // metadata block.
                usedBytes += LAYOUT.blockSize();
            }

            storedBytes += writeSize;
            usedBytes += LAYOUT.blockSize();
            checkSnapshot(c, storedBytes, usedBytes, reservedBytes, allocatedBytes, ACTUAL_MAX_SIZE);
        }

        AssertExtensions.assertThrows("Expecting cache to be full.",
                () -> c.insert(new ByteArraySegment(new byte[1])),
                ex -> ex instanceof CacheFullException);

        // Invoking close() (@Cleanup) will also verify it freed all the memory.
    }

    /**
     * Tests the ability to execute operations in order. We will invoke {@link DirectMemoryCache#insert}, {@link DirectMemoryCache#append},
     * {@link DirectMemoryCache#get} and {@link DirectMemoryCache#delete} without overfilling the cache.
     */
    @Test
    public void testRegularOperations() {
        final byte[] data = new byte[LAYOUT.bufferSize() * 2];
        final int writeCount = (int) (ACTUAL_MAX_SIZE / LAYOUT.bufferSize()) * (LAYOUT.blocksPerBuffer() - 1);
        rnd.nextBytes(data);
        val entryData = new HashMap<Integer, Map.Entry<Integer, Integer>>(); // Key=Address, Value={Offset, Length}

        @Cleanup
        val c = new TestCache();

        // Insert until we can no longer insert.
        CacheSnapshot cs = c.getSnapshot();
        while ((cs = c.getSnapshot()).getUsedBytes() < ACTUAL_MAX_SIZE) {
            int offset = rnd.nextInt(data.length - 1);
            int length = (int) Math.min(cs.getMaxBytes() - cs.getUsedBytes(), rnd.nextInt(data.length - offset));
            int address = c.insert(new ByteArraySegment(data, offset, length));
            entryData.put(address, new AbstractMap.SimpleEntry<>(offset, length));
        }

        // Verify we've actually filled up the cache.
        AssertExtensions.assertThrows(
                "Cache not full.",
                () -> c.insert(new ByteArraySegment(data, 0, 1)),
                ex -> ex instanceof CacheFullException);

        // Append to all those entries that we can append to.
        for (val e : entryData.entrySet()) {
            int address = e.getKey();
            int offset = e.getValue().getKey();
            int length = e.getValue().getValue();
            int appendableLength = c.getAppendableLength(length);

            AssertExtensions.assertThrows(
                    "append() accepted input that does not fit in last block.",
                    () -> c.append(address, length, new ByteArraySegment(data, 0, appendableLength + 1)),
                    ex -> ex instanceof IllegalArgumentException);

            val a = c.append(address, length, new ByteArraySegment(data, offset + length, appendableLength));
            Assert.assertEquals("Unexpected number of bytes appended.", appendableLength, a);
            e.getValue().setValue(length + a);
        }

        // We expect the cache to be full at this point.
        int reservedBytes = BUFFER_COUNT * LAYOUT.blockSize();
        checkSnapshot(c, ACTUAL_MAX_SIZE - reservedBytes, ACTUAL_MAX_SIZE, (long) reservedBytes, ACTUAL_MAX_SIZE, ACTUAL_MAX_SIZE);

        // Read all the data
        checkData(c, entryData, data);

        // Delete half the entries.
        int targetSize = entryData.size() / 2;
        val deletedData = new HashMap<Integer, Map.Entry<Integer, Integer>>();
        int deletedBlockCount = 0; // Keep track of how many blocks we deleted from when we are full - it's easier to track this way.
        for (val e : entryData.entrySet()) {
            int address = e.getKey();
            int offset = e.getValue().getKey();
            int length = e.getValue().getValue();
            deletedBlockCount += length / LAYOUT.blockSize();
            c.delete(address);
            Assert.assertNull("get() returned valid data for deleted entry.", c.get(address));

            deletedData.put(address, new AbstractMap.SimpleEntry<>(offset, length));
            if (deletedData.size() >= targetSize) {
                break;
            }
        }
        deletedData.keySet().forEach(entryData::remove);

        // Verify the cache has been freed of all of them.
        int deletedBytes = deletedBlockCount * LAYOUT.blockSize();
        checkSnapshot(c, ACTUAL_MAX_SIZE - reservedBytes - deletedBytes, ACTUAL_MAX_SIZE - deletedBytes,
                (long) reservedBytes, ACTUAL_MAX_SIZE, ACTUAL_MAX_SIZE);

        // Replace all existing entries with the ones we just deleted.
        val toReplace = new ArrayDeque<>(entryData.keySet());
        val replacements = new ArrayDeque<>(deletedData.values());
        while (!toReplace.isEmpty() && !replacements.isEmpty()) {
            val address = toReplace.removeFirst();
            val replacement = replacements.removeFirst();
            val newAddress = c.replace(address, new ByteArraySegment(data, replacement.getKey(), replacement.getValue()));
            val r = entryData.remove(address);
            deletedBlockCount += r.getValue() / LAYOUT.blockSize(); // Account for this entry's removal.
            entryData.put(newAddress, replacement);
            deletedBlockCount -= replacement.getValue() / LAYOUT.blockSize(); // Account for this entry's replacement.
        }

        // Verify final data.
        checkData(c, entryData, data);

        // Verify the cache has as many bytes as we think it has.
        deletedBytes = deletedBlockCount * LAYOUT.blockSize();
        checkSnapshot(c, ACTUAL_MAX_SIZE - reservedBytes - deletedBytes, ACTUAL_MAX_SIZE - deletedBytes,
                (long) reservedBytes, ACTUAL_MAX_SIZE, ACTUAL_MAX_SIZE);

        // Delete the rest and verify everything has been cleared.
        entryData.keySet().forEach(c::delete);
        checkSnapshot(c, 0L, (long) reservedBytes, (long) reservedBytes, ACTUAL_MAX_SIZE, ACTUAL_MAX_SIZE);
    }

    /**
     * Tests the ability to execute operations in random order. At each step we will either be inserting, appending or
     * deleting, and we will always be reading. We will not overfill the cache.
     */
    @Test
    public void testRandomOperations() {
        final byte[] data = new byte[LAYOUT.bufferSize() * 2];
        final int iterations = 200;
        @Cleanup
        val c = new TestCache();
        val addresses = new ArrayList<Integer>();
        val contents = new HashMap<Integer, Map.Entry<Integer, Integer>>(); // Key=Adddress, Value={StartOffset, Length}.

        long storedBytes = 0;
        for (int i = 0; i < iterations; i++) {
            // Add with 60% probability, but only if we have capacity or are empty.
            val s = c.getSnapshot();
            val freeBytes = s.getMaxBytes() - s.getUsedBytes();
            boolean add = freeBytes > 0 && rnd.nextInt(100) < 60 || addresses.isEmpty();
            if (add) {
                // Write a new entry of arbitrary length.
                int offset = rnd.nextInt(data.length - 1);
                int length = (int) Math.min(freeBytes, rnd.nextInt(data.length - offset));
                val address = c.insert(new ByteArraySegment(data, offset, length));
                storedBytes += length;
                addresses.add(address);
                contents.put(address, new AbstractMap.SimpleImmutableEntry(offset, length));
            } else {
                // Pick an arbitrary entry and remove it.
                int address = addresses.remove(rnd.nextInt(addresses.size()));
                val length = contents.remove(address).getValue();
                c.delete(address);
                storedBytes -= length;
                Assert.assertNull("Entry was still accessible after deletion.", c.get(address));
            }

            checkSnapshot(c, storedBytes, null, null, null, null);
        }

        checkData(c, contents, data);
    }

    /**
     * Tests the ability to notify the caller that a cache is full and handle various situations.
     */
    @Test
    public void testCacheFull() {

    }

    /**
     * Tests the ability to roll back any modifications that may have been performed while writing or appending.
     */
    @Test
    public void testWriteErrors() {

    }

    private void checkData(TestCache c, HashMap<Integer, Map.Entry<Integer, Integer>> entryData, byte[] data) {
        for (val e : entryData.entrySet()) {
            int address = e.getKey();
            int offset = e.getValue().getKey();
            int length = e.getValue().getValue();
            checkData(c, address, data, offset, length);
        }
    }

    private void checkData(TestCache c, int address, byte[] data, int offset, int length) {
        val r = c.get(address).getCopy();
        AssertExtensions.assertArrayEquals("Unexpected data read for address " + address, data, offset, r, 0, length);
    }

    private void checkSnapshot(TestCache c, Long storedBytes, Long usedBytes, Long reservedBytes, Long allocatedBytes, Long maxBytes) {
        val s = c.getSnapshot();
        if (storedBytes != null) {
            Assert.assertEquals("Unexpected Snapshot.getStoredBytes().", (long) storedBytes, s.getStoredBytes());
        }
        if (usedBytes != null) {
            Assert.assertEquals("Unexpected Snapshot.getUsedBytes().", (long) usedBytes, s.getUsedBytes());
        }
        if (reservedBytes != null) {
            Assert.assertEquals("Unexpected Snapshot.getReservedBytes().", (long) reservedBytes, s.getReservedBytes());
        }
        if (allocatedBytes != null) {
            Assert.assertEquals("Snapshot.getAllocatedBytes() does not reflect actual memory usage.", (long) allocatedBytes, c.getDirectMemoryUsed());
            Assert.assertEquals("Unexpected Snapshot.getAllocatedBytes().", (long) allocatedBytes, s.getAllocatedBytes());
        }
        if (maxBytes != null) {
            Assert.assertEquals("Unexpected Snapshot.getMaxBytes().", (long) maxBytes, s.getMaxBytes());
        }
    }

    private static final class TestCache extends DirectMemoryCache {
        private UnpooledByteBufAllocator allocator;

        TestCache() {
            super(LAYOUT, REQUESTED_MAX_SIZE);
        }

        @Override
        public void close() {
            super.close();
            Assert.assertEquals("Memory has not been freed after closing.", 0, getDirectMemoryUsed());
        }

        long getDirectMemoryUsed() {
            return this.allocator.metric().usedDirectMemory();
        }

        @Override
        protected ByteBufAllocator createAllocator() {
            if (this.allocator == null) {
                this.allocator = new UnpooledByteBufAllocator(true, false);
            }

            return this.allocator;
        }
    }
}
