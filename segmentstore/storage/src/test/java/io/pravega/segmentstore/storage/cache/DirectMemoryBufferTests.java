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
package io.pravega.segmentstore.storage.cache;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.test.common.AssertExtensions;
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
    public void testAllocateClose() {
        // Initial buffer.
        @Cleanup
        val b = newBuffer();
        checkAllocatedMemory(0);
        Assert.assertFalse(b.isAllocated());
        Assert.assertTrue(b.hasCapacity());
        Assert.assertEquals(1, b.getUsedBlockCount());

        // Allocate it, and write something.
        val w1 = b.write(BufferView.empty(), CacheLayout.NO_ADDRESS);
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
                () -> b.read(LAYOUT.getBlockId(w1.getLastBlockAddress()), new ArrayList<>()),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability to write.
     */
    @Test
    public void testWrite() {
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
            val w1 = b.write(new ByteArraySegment(toWrite, 0, size1), CacheLayout.NO_ADDRESS);
            Assert.assertEquals("Unexpected value for w1.firstBlockId.", 1, w1.getFirstBlockId());
            int expectedWritten1 = Math.min(size1, maxUsableSize);
            Assert.assertEquals("Unexpected w1.writtenLength", expectedWritten1, w1.getWrittenLength());
            if (expectedWritten1 == maxUsableSize) {
                Assert.assertFalse("Unexpected result from hasCapacity() when filling up.", b.hasCapacity());
            }

            // Perform a second write and verify it won't override the first one.
            boolean hasCapacityBeforeSecondWrite = b.hasCapacity();
            int remainingCapacity = (LAYOUT.blocksPerBuffer() - b.getUsedBlockCount()) * LAYOUT.blockSize();

            int size2 = Math.max(0, size1 - secondWriteOffset);
            val w2 = b.write(new ByteArraySegment(toWrite, secondWriteOffset, size2), CacheLayout.NO_ADDRESS);
            int expectedWritten2 = 0;
            if (expectedWritten1 < maxUsableSize && hasCapacityBeforeSecondWrite) {
                // We have nothing remaining and still have capacity to write more.
                Assert.assertEquals("Unexpected value for w2.firstBlockId.",
                        LAYOUT.getBlockId(w1.getLastBlockAddress()) + 1, w2.getFirstBlockId());
                expectedWritten2 = Math.min(size2, remainingCapacity);
                Assert.assertEquals("Unexpected w2.writtenLength", expectedWritten2, w2.getWrittenLength());
            } else {
                Assert.assertNull("Unexpected result when trying to write to a full buffer.", w2);
            }

            // Verify we can retrieve the data, then delete it. We will reuse the buffer in the next iteration.
            checkData(b, LAYOUT.getBlockId(w1.getLastBlockAddress()), new ByteArraySegment(toWrite, 0, expectedWritten1));
            if (w2 != null) {
                checkData(b, LAYOUT.getBlockId(w2.getLastBlockAddress()), new ByteArraySegment(toWrite, secondWriteOffset, expectedWritten2));
                b.delete(LAYOUT.getBlockId(w2.getLastBlockAddress()));
            }
            b.delete(LAYOUT.getBlockId(w1.getLastBlockAddress()));
        }
    }

    /**
     * Tests the ability to handle write errors and rollback if necessary. This indirectly verifies the functionality of
     * the private method {@link DirectMemoryBuffer#rollbackWrite}.
     */
    @Test
    public void testWriteError() {
        final byte[] toWrite = new byte[3 * LAYOUT.blockSize()];
        rnd.nextBytes(toWrite);

        @Cleanup
        val b = newBuffer();
        int initialUsedCount = b.getUsedBlockCount();
        AssertExtensions.assertThrows(
                "Expecting ArrayIndexOutOfBoundsException.",
                () -> b.write(new CorruptedByteArraySegment(toWrite, toWrite.length + 2), CacheLayout.NO_ADDRESS),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        // Verify the getUsedBlockCount is unaffected.
        Assert.assertEquals("Unexpected used block count after rollback.", initialUsedCount, b.getUsedBlockCount());

        // Verify the rollback has properly re-chained the free blocks.
        for (int i = 1; i < LAYOUT.blocksPerBuffer(); i++) {
            val w = b.write(new ByteArraySegment(toWrite, 0, LAYOUT.blockSize()), CacheLayout.NO_ADDRESS);
            Assert.assertEquals("Unexpected block to write (free block re-chaining).", i, w.getFirstBlockId());
            Assert.assertEquals("Expected to have written only 1 block.", w.getFirstBlockId(), LAYOUT.getBlockId(w.getLastBlockAddress()));
        }
    }

    /**
     * Tests the ability to append.
     */
    @Test
    public void testAppend() {
        final byte[] toWrite = new byte[LAYOUT.bufferSize()];
        rnd.nextBytes(toWrite);
        final int multiBlockLength = LAYOUT.blockSize() + 1;

        @Cleanup
        val b = newBuffer();

        // Test with an empty block (which is the result of an empty entry).
        val emptyAddress = b.write(new ByteArraySegment(toWrite, 0, 0), CacheLayout.NO_ADDRESS).getLastBlockAddress();
        testAppend(b, LAYOUT.getBlockId(emptyAddress), 0, toWrite);

        // Test with a non-empty block (which is the result of a non-empty entry).
        val multiBlockAddress = b.write(new ByteArraySegment(toWrite, 0, multiBlockLength), CacheLayout.NO_ADDRESS).getLastBlockAddress();
        testAppend(b, LAYOUT.getBlockId(multiBlockAddress), multiBlockLength, toWrite);

        // Test length-conditional appends.
        val conditionalAddress = b.write(new ByteArraySegment(toWrite, 0, 1), CacheLayout.NO_ADDRESS).getLastBlockAddress();
        AssertExtensions.assertThrows(
                "tryAppend() did not fail for expectedLastBlockLength mismatch.",
                () -> b.tryAppend(LAYOUT.getBlockId(conditionalAddress), 2, new ByteArraySegment(toWrite, 0, 1)),
                ex -> ex instanceof IncorrectCacheEntryLengthException);
    }

    private void testAppend(DirectMemoryBuffer b, int lastBlockId, int entryLength, byte[] toWrite) {
        int blockLength = entryLength % LAYOUT.blockSize();
        int remainingLength = LAYOUT.blockSize() - blockLength;
        int lastBlockEntryOffset = entryLength - entryLength % LAYOUT.blockSize();
        int initialUsedBlockCount = b.getUsedBlockCount();
        while (remainingLength >= 0) {
            val appendedLength = b.tryAppend(lastBlockId, blockLength, new ByteArraySegment(toWrite, lastBlockEntryOffset + blockLength, 1));
            Assert.assertEquals("Unexpected appendedLength.", Math.min(remainingLength, 1), appendedLength);
            remainingLength--;
            blockLength++;
        }

        int expectedFinalLength = lastBlockEntryOffset + LAYOUT.blockSize();
        checkData(b, lastBlockId, new ByteArraySegment(toWrite, 0, expectedFinalLength));
        Assert.assertEquals("Not expecting used block count to change.", initialUsedBlockCount, b.getUsedBlockCount());
    }

    /**
     * Tests the ability to delete.
     */
    @Test
    public void testDelete() {
        final byte[] toWrite = new byte[LAYOUT.bufferSize()];
        rnd.nextBytes(toWrite);
        final int multiBlockLength = LAYOUT.blockSize() + 1;

        @Cleanup
        val b = newBuffer();
        int expectedUsedBlockCount = 1;

        val empty = b.write(new ByteArraySegment(toWrite, 0, 0), CacheLayout.NO_ADDRESS);
        expectedUsedBlockCount += 1;
        val multiBlock = b.write(new ByteArraySegment(toWrite, 0, multiBlockLength), CacheLayout.NO_ADDRESS);
        expectedUsedBlockCount += 2;
        val predecessorAddress = LAYOUT.calculateAddress(BUFFER_ID + 1, 2);
        val multiBuf = b.write(new ByteArraySegment(toWrite, 0, 2 * LAYOUT.blockSize()), predecessorAddress);
        expectedUsedBlockCount += 2;

        val emptyDelete = b.delete(LAYOUT.getBlockId(empty.getLastBlockAddress()));
        Assert.assertEquals(0, emptyDelete.getDeletedLength());
        Assert.assertEquals(CacheLayout.NO_ADDRESS, emptyDelete.getPredecessorAddress());
        expectedUsedBlockCount -= 1;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());

        val multiBlockDelete = b.delete(LAYOUT.getBlockId(multiBlock.getLastBlockAddress()));
        Assert.assertEquals(multiBlockLength, multiBlockDelete.getDeletedLength());
        Assert.assertEquals(CacheLayout.NO_ADDRESS, multiBlockDelete.getPredecessorAddress());
        expectedUsedBlockCount -= 2;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());

        val multiBufDelete = b.delete(LAYOUT.getBlockId(multiBuf.getLastBlockAddress()));
        Assert.assertEquals(2 * LAYOUT.blockSize(), multiBufDelete.getDeletedLength());
        Assert.assertEquals(predecessorAddress, multiBufDelete.getPredecessorAddress());
        expectedUsedBlockCount -= 2;
        Assert.assertEquals(expectedUsedBlockCount, b.getUsedBlockCount());
    }

    /**
     * Tests random operations on the buffer. This verifies that we can successfully allocate and deallocate buffers and
     * "jump" over issues.
     */
    @Test
    public void testRandomOperations() {
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
                val w = b.write(new ByteArraySegment(toWrite, offset, length), CacheLayout.NO_ADDRESS);
                usedBlocks = Math.min(LAYOUT.blocksPerBuffer(), usedBlocks + (length == 0 ? 1 : length / LAYOUT.blockSize() + 1));
                Assert.assertEquals("Unexpected used blocks after write.", usedBlocks, b.getUsedBlockCount());
                int blockId = LAYOUT.getBlockId(w.getLastBlockAddress());
                blocks.add(blockId);
                contents.put(blockId, new AbstractMap.SimpleImmutableEntry<>(offset, w.getWrittenLength()));
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
    public void testFreeBlockManagement() {
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
                val w = b.write(new ByteArraySegment(data, 0, writeLength), CacheLayout.NO_ADDRESS);
                int firstBlockId = w.getFirstBlockId();
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
                    val d = b.delete(e.getLastBlockId());
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
        ByteBuf result = readBuffers.size() == 1
                ? readBuffers.get(0)
                : new CompositeByteBuf(this.allocator, false, readBuffers.size(), Lists.reverse(readBuffers));
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

    private static class CorruptedByteArraySegment extends ByteArraySegment {
        private final int fakeLength;

        CorruptedByteArraySegment(byte[] array, int fakeLength) {
            super(array);
            this.fakeLength = fakeLength;
        }

        @Override
        public int getLength() {
            return this.fakeLength;
        }
    }

    //endregion
}
