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
import java.util.ArrayList;
import java.util.Random;
import lombok.Cleanup;
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
                () -> b.read(w1.getFirstBlockAddress(), new ArrayList<>()),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability to write.
     */
    @Test
    public void testWrite() throws Exception {
        final byte[] toWrite = new byte[LAYOUT.bufferSize() + 3 * LAYOUT.blockSize()];
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

            // Verify we can retrieve the data, then delete it.
            checkData(b, LAYOUT.getBlockId(w1.getFirstBlockAddress()), new ByteArraySegment(toWrite, 0, size1 - expectedRemaining1));
            if (w2 != null) {
                checkData(b, LAYOUT.getBlockId(w2.getFirstBlockAddress()), new ByteArraySegment(toWrite, secondWriteOffset, size2 - expectedRemaining2));
                b.delete(LAYOUT.getBlockId(w2.getFirstBlockAddress()));
            }
            b.delete(LAYOUT.getBlockId(w1.getFirstBlockAddress()));
        }
    }

    private void checkData(DirectMemoryBuffer b, int blockId, ArrayView expectedData) {
        val data1 = read(b, LAYOUT.getBlockId(blockId)).getCopy();
        Assert.assertEquals("Unexpected length.", expectedData.getLength(), data1.length);
        AssertExtensions.assertArrayEquals("", expectedData.array(), expectedData.arrayOffset(), data1, 0, expectedData.getLength());
    }

    /**
     * Tests the ability to append.
     */
    @Test
    public void testAppend() {

    }

    /**
     * Tests the ability to delete.
     */
    @Test
    public void testDelete() {

    }

    /**
     * Tests the {@link DirectMemoryBuffer#setSuccessor} method.
     */
    @Test
    public void testSetSuccessor() {

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
}
