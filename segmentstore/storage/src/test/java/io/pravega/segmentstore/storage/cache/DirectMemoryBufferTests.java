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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link DirectMemoryBuffer} class.
 */
public class DirectMemoryBufferTests {
    private static final int BUFFER_ID = 1;
    private static final CacheLayout LAYOUT = new CacheLayout.DefaultLayout();
    private final Random rnd = new Random(0);
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true, false);

    /**
     * Tests the ability to allocate the backing buffer on demand and release it when invoking {@link DirectMemoryBuffer#close()} .
     */
    @Test
    public void testAllocateClose() throws IOException {
        // Initial buffer.
        @Cleanup
        val b = newBuffer();
        checkAllocatedMemory(0);
        Assert.assertFalse(b.isAllocated());
        Assert.assertTrue(b.hasCapacity());
        Assert.assertEquals(1, b.getUsedBlockCount());

        // Allocate it, and write something.
        val w1 = b.write(new ByteArrayInputStream(new byte[0]), 0, true);
        checkAllocatedMemory(LAYOUT.bufferSize());
        Assert.assertTrue(b.isAllocated());
        Assert.assertTrue(b.hasCapacity());
        Assert.assertEquals(2, b.getUsedBlockCount());

        // Close it.
        b.close();
        checkAllocatedMemory(0);
        Assert.assertFalse(b.isAllocated());
        Assert.assertFalse(b.hasCapacity());
        Assert.assertEquals(-1, b.getUsedBlockCount());
        AssertExtensions.assertThrows("Was able to read after closing",
                () -> b.read(LAYOUT.getBlockId(w1.getFirstBlockAddress()), new ArrayList<>()),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability to write.
     */
    @Test
    public void testWrite() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.bufferSize() + 3 * LAYOUT.blockSize()];
        rnd.nextBytes(toWrite);
        final int sizeIncrement = 2 * LAYOUT.blockSize() + LAYOUT.blockSize() / 2;
        final int secondWriteOffset = 1;
        final int maxUsableSize = LAYOUT.bufferSize() - LAYOUT.blockSize(); // Exclude metadata.

        // We reuse the buffer multiple times. We want to also verify there is no leak between each run.
        @Cleanup
        val b = newBuffer();
        for (int size1 = 0; size1 < toWrite.length; size1 += sizeIncrement) {
            Assert.assertEquals("Expected buffer to be clean.", 1, b.getUsedBlockCount());
            val w1 = b.write(new ByteArrayInputStream(toWrite, 0, size1), size1, true);
            Assert.assertEquals("Unexpected buffer id for w1.firstBlockAddress.", BUFFER_ID, LAYOUT.getBufferId(w1.getFirstBlockAddress()));
            Assert.assertEquals("Unexpected block id for w1.firstBlockAddress.", 1, LAYOUT.getBlockId(w1.getFirstBlockAddress()));
            int expectedRemaining1 = size1 > maxUsableSize ? size1 - maxUsableSize : 0;
            Assert.assertEquals("Unexpected w1.remainingLength", expectedRemaining1, w1.getRemainingLength());
            if (expectedRemaining1 > 0) {
                Assert.assertFalse("Unexpected result from hasCapacity() when filling up.", b.hasCapacity());
            }

            // Perform a second write and verify it won't override the first one.
            boolean hasCapacityBeforeSecondWrite = b.hasCapacity();
            int remainingCapacity = (LAYOUT.blocksPerBuffer() - b.getUsedBlockCount()) * LAYOUT.blockSize();

            int size2 = Math.max(0, size1 - secondWriteOffset);
            val w2 = b.write(new ByteArrayInputStream(toWrite, secondWriteOffset, size2), size2, false);
            int expectedRemaining2 = 0;
            if (expectedRemaining1 == 0 && hasCapacityBeforeSecondWrite) {
                // We have nothing remaining and still have capacity to write more.
                Assert.assertEquals("Unexpected buffer id for w2.firstBlockAddress.", BUFFER_ID, LAYOUT.getBufferId(w2.getFirstBlockAddress()));
                Assert.assertEquals("Unexpected block id for w2.firstBlockAddress.",
                        LAYOUT.getBlockId(w1.getLastBlockAddress()) + 1, LAYOUT.getBlockId(w2.getFirstBlockAddress()));
                expectedRemaining2 = size2 > remainingCapacity ? size2 - remainingCapacity : 0;
                Assert.assertEquals("Unexpected w2.remainingLength", expectedRemaining2, w2.getRemainingLength());
            } else {
                Assert.assertNull("Unexpected result when trying to write to a full buffer.", w2);
            }

            // Verify we can retrieve the data, then delete it. We will reuse the buffer in the next iteration.
            checkData(b, LAYOUT.getBlockId(w1.getFirstBlockAddress()), new ByteArraySegment(toWrite, 0, size1 - expectedRemaining1));
            if (w2 != null) {
                checkData(b, LAYOUT.getBlockId(w2.getFirstBlockAddress()), new ByteArraySegment(toWrite, secondWriteOffset, size2 - expectedRemaining2));
                b.delete(LAYOUT.getBlockId(w2.getFirstBlockAddress()));
            }
            b.delete(LAYOUT.getBlockId(w1.getFirstBlockAddress()));
        }
    }

    /**
     * Tests the ability to append.
     */
    @Test
    public void testAppend() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.bufferSize()];
        rnd.nextBytes(toWrite);
        final int multiBlockLength = LAYOUT.blockSize() + 1;

        @Cleanup
        val b = newBuffer();

        // Test with an empty block (which is the result of an empty entry).
        val emptyAddress = b.write(new ByteArrayInputStream(toWrite), 0, true).getFirstBlockAddress();
        testAppend(b, LAYOUT.getBlockId(emptyAddress), 0, toWrite);

        // Test with a non-empty block (which is the result of a non-empty entry).
        val multiBlockAddress = b.write(new ByteArrayInputStream(toWrite), multiBlockLength, true).getFirstBlockAddress();
        testAppend(b, LAYOUT.getBlockId(multiBlockAddress), multiBlockLength, toWrite);

        // Test length-conditional appends.
        val conditionalAddress = b.write(new ByteArrayInputStream(toWrite), 1, true).getFirstBlockAddress();
        AssertExtensions.assertThrows(
                "tryAppend() did not fail for expectedLastBlockLength mismatch.",
                () -> b.tryAppend(LAYOUT.getBlockId(conditionalAddress), 2, new ByteArrayInputStream(toWrite), 1),
                ex -> ex instanceof IncorrectCacheEntryLengthException);

        // Test multi-Buffer entries.
        val multiBuf = b.write(new ByteArrayInputStream(toWrite), 2 * LAYOUT.blockSize(), true);
        val successorAddress = LAYOUT.calculateAddress(BUFFER_ID + 1, 2);
        b.setSuccessor(multiBuf.getLastBlockAddress(), successorAddress);
        val multiBufAppend = b.tryAppend(LAYOUT.getBlockId(multiBuf.getFirstBlockAddress()), 456, new ByteArrayInputStream(toWrite), 1);
        Assert.assertEquals("Not expecting any appended data for multi-buffer append.", 0, multiBufAppend.getAppendedLength());
        Assert.assertEquals("Unexpected next block address for multi-buffer append.", successorAddress, multiBufAppend.getNextBlockAddress());
    }

    private void testAppend(DirectMemoryBuffer b, int firstBlockId, int entryLength, byte[] toWrite) throws Exception {
        int blockLength = entryLength % LAYOUT.blockSize();
        int remainingLength = LAYOUT.blockSize() - blockLength;
        int lastBlockEntryOffset = entryLength - entryLength % LAYOUT.blockSize();
        int initialUsedBlockCount = b.getUsedBlockCount();
        while (remainingLength >= 0) {
            val a = b.tryAppend(firstBlockId, blockLength, new ByteArrayInputStream(toWrite, lastBlockEntryOffset + blockLength, 1), 1);
            Assert.assertEquals("Unexpected appendedLength.", Math.min(remainingLength, 1), a.getAppendedLength());
            Assert.assertEquals("Not expecting a successor address.", CacheLayout.NO_ADDRESS, a.getNextBlockAddress());
            remainingLength--;
            blockLength++;
        }

        int expectedFinalLength = lastBlockEntryOffset + LAYOUT.blockSize();
        checkData(b, firstBlockId, new ByteArraySegment(toWrite, 0, expectedFinalLength));
        Assert.assertEquals("Not expecting used block count to change.", initialUsedBlockCount, b.getUsedBlockCount());
    }

    /**
     * Tests the ability to delete.
     */
    @Test
    public void testDelete() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.bufferSize()];
        rnd.nextBytes(toWrite);
        final int multiBlockLength = LAYOUT.blockSize() + 1;

        @Cleanup
        val b = newBuffer();
        int expectedUsedBlockCount = 1;

        val empty = b.write(new ByteArrayInputStream(toWrite), 0, true);
        expectedUsedBlockCount += 1;
        val multiBlock = b.write(new ByteArrayInputStream(toWrite), multiBlockLength, true);
        expectedUsedBlockCount += 2;
        val multiBuf = b.write(new ByteArrayInputStream(toWrite), 2 * LAYOUT.blockSize(), true);
        expectedUsedBlockCount += 2;
        val successorAddress = LAYOUT.calculateAddress(BUFFER_ID + 1, 2);
        b.setSuccessor(multiBuf.getLastBlockAddress(), successorAddress);

        val emptyDelete = b.delete(LAYOUT.getBlockId(empty.getFirstBlockAddress()));
        Assert.assertEquals(0, emptyDelete.getDeletedLength());
        Assert.assertEquals(CacheLayout.NO_ADDRESS, emptyDelete.getSuccessorAddress());
        expectedUsedBlockCount -= 1;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());

        val multiBlockDelete = b.delete(LAYOUT.getBlockId(multiBlock.getFirstBlockAddress()));
        Assert.assertEquals(multiBlockLength, multiBlockDelete.getDeletedLength());
        Assert.assertEquals(CacheLayout.NO_ADDRESS, multiBlockDelete.getSuccessorAddress());
        expectedUsedBlockCount -= 2;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());

        val multiBufDelete = b.delete(LAYOUT.getBlockId(multiBuf.getFirstBlockAddress()));
        Assert.assertEquals(2 * LAYOUT.blockSize(), multiBufDelete.getDeletedLength());
        Assert.assertEquals(successorAddress, multiBufDelete.getSuccessorAddress());
        expectedUsedBlockCount -= 2;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());
    }

    /**
     * Tests the {@link DirectMemoryBuffer#setSuccessor} method.
     */
    @Test
    public void testSetSuccessor() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.blockSize()];
        val successorAddress = LAYOUT.calculateAddress(BUFFER_ID + 1, 1);

        @Cleanup
        val b = newBuffer();
        val nonFullWrite = b.write(new ByteArrayInputStream(toWrite), 1, true);
        AssertExtensions.assertThrows(
                "setSuccessor allowed incorrect buffer id",
                () -> b.setSuccessor(LAYOUT.calculateAddress(BUFFER_ID + 1, LAYOUT.getBlockId(nonFullWrite.getLastBlockAddress())), successorAddress),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "setSuccessor allowed unused block id",
                () -> b.setSuccessor(LAYOUT.calculateAddress(BUFFER_ID, LAYOUT.getBlockId(nonFullWrite.getLastBlockAddress()) + 1), successorAddress),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "setSuccessor worked on non-full block",
                () -> b.setSuccessor(nonFullWrite.getLastBlockAddress(), successorAddress),
                ex -> ex instanceof IllegalArgumentException);

        Assert.assertEquals("Unexpected succesor retrieved via read() before setting it.",
                CacheLayout.NO_ADDRESS, b.read(LAYOUT.getBlockId(nonFullWrite.getFirstBlockAddress()), new ArrayList<>()));

        val fullWrite = b.write(new ByteArrayInputStream(toWrite), LAYOUT.blockSize(), true);
        b.setSuccessor(fullWrite.getLastBlockAddress(), successorAddress);
        Assert.assertEquals("Unexpected succesor retrieved via read().",
                successorAddress, b.read(LAYOUT.getBlockId(fullWrite.getFirstBlockAddress()), new ArrayList<>()));
    }

    /**
     * Tests random operations on the buffer. This verifies that we can successfully allocate and deallocate buffers and
     * "jump" over issues.
     */
    @Test
    public void testRandomOperations() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.bufferSize()];
        final int iterations = 100;
        @Cleanup
        val b = newBuffer();
        val blocks = new ArrayList<Integer>();
        val contents = new HashMap<Integer, Map.Entry<Integer, Integer>>(); // Key=Adddress, Value={StartOffset, Length}.

        for (int i = 0; i < iterations; i++) {
            // Add with 60% probability, but only if we have capacity or are empty.
            boolean add = b.hasCapacity() && rnd.nextInt(100) < 60 || blocks.isEmpty();
            int usedBlocks = b.getUsedBlockCount();
            if (add) {
                // Write a new entry of arbitrary length.
                int offset = rnd.nextInt(toWrite.length - 1);
                int length = rnd.nextInt(toWrite.length - offset);
                val w = b.write(new ByteArrayInputStream(toWrite, offset, length), length, true);
                usedBlocks = Math.min(LAYOUT.blocksPerBuffer(), usedBlocks + (length == 0 ? 1 : length / LAYOUT.blockSize() + 1));
                Assert.assertEquals("Unexpected used blocks after write.", usedBlocks, b.getUsedBlockCount());
                int blockId = LAYOUT.getBlockId(w.getFirstBlockAddress());
                blocks.add(blockId);
                contents.put(blockId, new AbstractMap.SimpleImmutableEntry<>(offset, length - w.getRemainingLength()));
            } else {
                // Pick an arbitrary entry and remove it.
                int blockId = blocks.remove(rnd.nextInt(blocks.size()));
                contents.remove(blockId);
                b.delete(blockId);
                val readBuffers = new ArrayList<ByteBuf>();

                // Verify that we cannot read anything.
                val readResult = b.read(blockId, readBuffers);
                Assert.assertEquals("Unexpected result from read() for deleted block.", CacheLayout.NO_ADDRESS, readResult);
                Assert.assertEquals("Not expecting to read anything for a deleted block.", 0, readBuffers.size());
            }

            // Check all remaining entries.
            for (val e : contents.entrySet()) {
                checkData(b, e.getKey(), new ByteArraySegment(toWrite, e.getValue().getKey(), e.getValue().getValue()));
            }
        }
    }

    /**
     * Tests the ability to properly allocate and deallocate Buffer-Blocks and re-link empty blocks together. This tests
     * the working of the private method {@link DirectMemoryBuffer#deallocateBlocks}.
     *
     * This test begins by filling up all the buffer with single-block entries, then deleting every other one. Next it
     * fills the leftover capacity with two-block entries, and again deletes every other entry (of all, not just the new
     * ones). This is repeated until we make the largest possible write (all blocks at once).
     *
     * At each step the allocated blocks are checked against a parallel-maintained structure whose purpose is to validate
     * the buffer is used as expected and that we do not lose free blocks once we remove entries.
     */
    @Test
    public void testFreeBlockManagement() throws Exception {
        final int maxWriteBlockSize = LAYOUT.blocksPerBuffer() - 1; // Excluding metadata block.
        final byte[] data = new byte[maxWriteBlockSize * LAYOUT.blockSize()];

        // Keep a sorted list of free blocks.
        val freeBlocks = new ArrayList<Integer>();
        IntStream.range(1, LAYOUT.blocksPerBuffer()).forEach(freeBlocks::add);
        freeBlocks.sort(Comparator.reverseOrder());
        //val writtenEntries = new HashMap<Integer, WriteEntry>(); // Key=Starting Block Id.
        val writtenEntries = new PriorityQueue<WriteEntry>(Comparator.comparingInt(WriteEntry::getFirstBlockId));

        @Cleanup
        val b = newBuffer();
        // We repeat writing, at each time filling up the buffer with equal-sized writes. We begin with 1 block writes
        // until we make writes equalling the entire buffer length.
        for (int writeBlocks = 1; writeBlocks <= maxWriteBlockSize; writeBlocks++) {
            int writeLength = writeBlocks * LAYOUT.blockSize();

            // Fill up the buffer with writes of this size.
            while (!freeBlocks.isEmpty()) {
                Assert.assertTrue("Expecting buffer to have capacity left.", b.hasCapacity());
                val w = b.write(new ByteArrayInputStream(data, 0, writeLength), writeLength, true);
                int firstBlockId = LAYOUT.getBlockId(w.getFirstBlockAddress());
                int lastBlockId = LAYOUT.getBlockId(w.getLastBlockAddress());

                // Determine how many blocks were actually expected and validate.
                val expectedBlocks = updateFreeBlocksAfterWriting(freeBlocks, writeBlocks);
                Assert.assertEquals("Unexpected First Block Id.", expectedBlocks.getFirstBlockId(), firstBlockId);
                Assert.assertEquals("Unexpected Last Block Id.", expectedBlocks.getLastBlockId(), lastBlockId);
                Assert.assertEquals("Unexpected value from getUsedBlockCount.",
                        LAYOUT.blocksPerBuffer() - freeBlocks.size(), b.getUsedBlockCount());

                writtenEntries.add(expectedBlocks);
            }

            // Delete every 1 other.
            boolean delete = true;
            val entryIterator = writtenEntries.iterator();
            while (entryIterator.hasNext()) {
                WriteEntry e = entryIterator.next();
                if (delete) {
                    val d = b.delete(e.getFirstBlockId());
                    Assert.assertEquals("Expected whole blocks to be deleted.", 0, d.getDeletedLength() % LAYOUT.blockSize());
                    Assert.assertEquals("Unexpected number of blocks deleted.", e.blockIds.size(), d.getDeletedLength() / LAYOUT.blockSize());
                    updateFreeBlocksAfterDeletion(freeBlocks, e);
                    entryIterator.remove();
                }

                delete = !delete;
            }
        }
    }

    private void updateFreeBlocksAfterDeletion(ArrayList<Integer> freeBlocks, WriteEntry writeEntry) {
        freeBlocks.addAll(writeEntry.blockIds);
        freeBlocks.sort(Comparator.reverseOrder());
    }

    private WriteEntry updateFreeBlocksAfterWriting(ArrayList<Integer> freeBlocks, int writeBlockCount) {
        val writeBlocks = new ArrayList<Integer>();
        while (!freeBlocks.isEmpty() && writeBlockCount > 0) {
            writeBlocks.add(freeBlocks.remove(freeBlocks.size() - 1));
            writeBlockCount--;
        }

        return writeBlocks.isEmpty() ? null : new WriteEntry(writeBlocks);
    }

    private DirectMemoryBuffer newBuffer() {
        return new DirectMemoryBuffer(BUFFER_ID, this.allocator, LAYOUT);
    }

    private void checkAllocatedMemory(long expected) {
        Assert.assertEquals("Unexpected amount of memory used.", expected, this.allocator.metric().usedDirectMemory());
    }

    private BufferView read(DirectMemoryBuffer buffer, int blockId) {
        val readBuffers = new ArrayList<ByteBuf>();
        buffer.read(blockId, readBuffers);
        ByteBuf result = readBuffers.size() == 1 ? readBuffers.get(0) : new CompositeByteBuf(this.allocator, false, readBuffers.size(), readBuffers);
        return new ByteBufWrapper(result);
    }

    private void checkData(DirectMemoryBuffer b, int blockId, ArrayView expectedData) {
        val data1 = read(b, LAYOUT.getBlockId(blockId)).getCopy();
        Assert.assertEquals("Unexpected length.", expectedData.getLength(), data1.length);
        AssertExtensions.assertArrayEquals("Unexpected data.", expectedData.array(), expectedData.arrayOffset(), data1, 0, expectedData.getLength());
    }

    //region Helper Classes
    @RequiredArgsConstructor
    private static class WriteEntry {
        final ArrayList<Integer> blockIds;

        int getFirstBlockId() {
            return this.blockIds.get(0);
        }

        int getLastBlockId() {
            return this.blockIds.get(this.blockIds.size() - 1);
        }

        double getFragmentation() {
            int spread = getLastBlockId() - getFirstBlockId() + 1;
            return 1.0 - (double) this.blockIds.size() / spread;
        }

        @Override
        public String toString() {
            return String.format("%d-%d (Count=%d, Fragmentation=%.1f%%)", getFirstBlockId(), getLastBlockId(),
                    this.blockIds.size(), getFragmentation() * 100);
        }
    }

    //endregion
}
