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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.pravega.common.Exceptions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Stack;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A block-based, direct memory buffer used by {@link DirectMemoryCache}.
 */
@ThreadSafe
class DirectMemoryBuffer implements AutoCloseable {
    //region Members

    /**
     * Buffer Id.
     */
    @Getter
    private final int id;
    private final CacheLayout layout;
    private final ByteBufAllocator allocator;
    @GuardedBy("this")
    private ByteBuf buf;
    @GuardedBy("this")
    private int usedBlockCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link DirectMemoryBuffer} class. This does not allocate any memory for the buffer
     * yet (that will be done on its first use).
     *
     * @param bufferId  If of this buffer. Must be a non-negative value less than `layout.maxBufferCount()`.
     * @param allocator A {@link ByteBufAllocator} to use for allocating buffers.
     * @param layout    A {@link CacheLayout} that is used to organize the buffer.
     */
    DirectMemoryBuffer(int bufferId, @NonNull ByteBufAllocator allocator, @NonNull CacheLayout layout) {
        Preconditions.checkArgument(bufferId >= 0 && bufferId < layout.maxBufferCount());

        this.allocator = allocator;
        this.layout = layout;
        this.id = bufferId;
        this.usedBlockCount = 1; // Metadata Block.
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public synchronized void close() {
        if (this.buf != null && this.buf.refCnt() > 0) {
            this.buf.release();
            this.buf = null;
        }

        this.usedBlockCount = -1;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the number of used Blocks in this buffer.
     *
     * @return The number of used blocks.
     */
    synchronized int getUsedBlockCount() {
        return this.usedBlockCount;
    }

    /**
     * Gets a value indicating whether this buffer has allocated any memory yet.
     *
     * @return The result.
     */
    synchronized boolean isAllocated() {
        return this.buf != null;
    }

    /**
     * Gets a value indicating whether this buffer has any capacity to add new blocks (NOTE: there may be capacity for
     * appending to existing blocks, but this method refers to new blocks).
     *
     * @return True if at least one block is free, false otherwise.
     */
    synchronized boolean hasCapacity() {
        return this.usedBlockCount > 0 && this.usedBlockCount < this.layout.blocksPerBuffer();
    }

    @Override
    public synchronized String toString() {
        return String.format("Id=%d, UsedBlockCount=%d", this.id, this.usedBlockCount);
    }

    //endregion

    //region Buffer Implementation

    /**
     * Writes new data to this buffer, if {@link #hasCapacity()} is true.
     *
     * This will begin writing data to the first available block in the buffer, and filling blocks until either running
     * out of available blocks or all the data has been written.
     *
     * @param data               An {@link InputStream} representing the data to add. This InputStream will not be closed
     *                           or reset throughout the execution of this method, even in case of failure.
     * @param remainingLength    The length of the data to add.
     * @param predecessorAddress The address of the Buffer-Block that precedes the write to this Buffer. If none, this
     *                           should be set to {@link  CacheLayout#NO_ADDRESS}.
     * @return A {@link WriteResult} representing the result of the write, or null if {@link #hasCapacity()} is false.
     * If all the data has been written, then  {@link WriteResult#getRemainingLength()} will be 0.
     * @throws IOException If an {@link IOException} occurred while reading from `data`.
     */
    synchronized WriteResult write(InputStream data, int remainingLength, int predecessorAddress) throws IOException {
        if (this.usedBlockCount >= this.layout.blocksPerBuffer()) {
            // Full
            return null;
        }

        int lastBlockLength = remainingLength % this.layout.blockSize();
        if (lastBlockLength == 0 && remainingLength > 0) {
            lastBlockLength = this.layout.blockSize();
        }

        ByteBuf metadataBuf = getBlockBuffer(0);
        long blockMetadata = metadataBuf.getLong(0);
        int blockId = this.layout.getNextFreeBlockId(blockMetadata);
        assert blockId != CacheLayout.NO_BLOCK_ID;
        final int firstBlockId = blockId;
        try {
            while (blockId != CacheLayout.NO_BLOCK_ID) {
                int bufIndex = blockId * this.layout.blockMetadataSize();
                blockMetadata = metadataBuf.getLong(bufIndex);
                assert !this.layout.isUsedBlock(blockMetadata);
                int blockLength = Math.min(remainingLength, this.layout.blockSize());
                if (blockLength > 0) {
                    blockLength = writeBlock(getBlockBuffer(blockId), 0, data, blockLength);
                }

                // Update block metadata.
                long metadata = this.layout.newBlockMetadata(CacheLayout.NO_BLOCK_ID, blockLength, predecessorAddress);
                metadataBuf.setLong(bufIndex, metadata);
                this.usedBlockCount++;

                // Move on to the next block to write.
                predecessorAddress = this.layout.calculateAddress(this.id, blockId);
                blockId = this.layout.getNextFreeBlockId(blockMetadata);
                remainingLength -= blockLength;

                if (remainingLength <= 0) {
                    // We are done.
                    break;
                }
            }
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // We wrote something, but got interrupted. We need to clean up whatever we wrote so we don't leave
                // unreferenced data in the buffer.
                rollbackWrite(this.layout.getBlockId(predecessorAddress), blockId);
            }

            throw ex;
        }

        // Update the root metadata.
        blockMetadata = metadataBuf.getLong(0);
        blockMetadata = this.layout.setNextFreeBlockId(blockMetadata, blockId);
        metadataBuf.setLong(0, blockMetadata);

        return new WriteResult(remainingLength, predecessorAddress, firstBlockId);
    }

    /**
     * Attempts to append some data to an entry.
     *
     * @param blockId        The id of the Buffer-Block to append that resides in this buffer.
     * @param expectedLength The expected length of the Buffer-Block to append to. If this value does not match what is
     *                       stored in that Buffer-Block metdata, the append will not execute.
     * @param data           An {@link InputStream} representing the data to append. This InputStream will not be closed
     *                       or reset throughout the execution of this method, even in case of failure.
     * @param maxLength      The maximum length to append.
     * @return The number of bytes appended.
     * @throws IOException                        If an {@link IOException} has occurred while reading from `data`.
     * @throws IncorrectCacheEntryLengthException If `expectedLastBlockLength` differs from the last block's length.
     */
    synchronized int tryAppend(int blockId, int expectedLength, InputStream data, int maxLength) throws IOException {
        validateBlockId(blockId, false);

        ByteBuf metadataBuf = getBlockBuffer(0);
        int bufIndex = blockId * this.layout.blockMetadataSize();
        long blockMetadata = metadataBuf.getLong(bufIndex);
        if (blockId == CacheLayout.NO_BLOCK_ID || !this.layout.isUsedBlock(blockMetadata)) {
            return 0;
        }

        // Append to the block, but only if the given length matches the actual one.
        int blockLength = this.layout.getLength(blockMetadata);
        if (blockLength != expectedLength) {
            throw new IncorrectCacheEntryLengthException(String.format(
                    "Incorrect last block length. Expected %s, given %s.", blockLength, expectedLength));
        }

        maxLength = Math.min(maxLength, this.layout.blockSize() - blockLength);
        blockLength += writeBlock(getBlockBuffer(blockId), blockLength, data, maxLength);

        // Update metadata.
        blockMetadata = this.layout.setLength(blockMetadata, blockLength);
        metadataBuf.setLong(bufIndex, blockMetadata);
        return blockLength - expectedLength;
    }

    /**
     * Reads from the buffer starting at the given Buffer-Block Id and moving backwards (following
     * {@link CacheLayout#getPredecessorAddress} for each encountered block.
     *
     * @param blockId     The id of the Buffer-Block to begin reading from.
     * @param readBuffers A list of {@link ByteBuf} to add read data to. The {@link ByteBuf}s will be added in reverse
     *                    order (from last to first).
     * @return The address of the previous Buffer-Block in the sequence, or {@link CacheLayout#NO_ADDRESS} if we have
     * reached the beginning of this entry.
     */
    synchronized int read(int blockId, List<ByteBuf> readBuffers) {
        validateBlockId(blockId, true);
        ByteBuf metadataBuf = getBlockBuffer(0);
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            int bufIndex = blockId * this.layout.blockMetadataSize();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            if (this.layout.isUsedBlock(blockMetadata)) {
                int blockLength = this.layout.getLength(blockMetadata);
                if (!readBuffers.isEmpty() && blockLength < this.layout.blockSize()) {
                    throw new CacheCorruptedException(String.format("Buffer %s, Block %s: Non-full, non-terminal block (length=%s).",
                            this.id, blockId, blockLength));
                }

                int predecessorAddress = this.layout.getPredecessorAddress(blockMetadata);
                readBuffers.add(getBlockBuffer(blockId).slice(0, Math.min(blockLength, this.layout.blockSize())).asReadOnly());
                if (predecessorAddress == CacheLayout.NO_ADDRESS || this.layout.getBufferId(predecessorAddress) != this.id) {
                    // We are done.
                    return predecessorAddress;
                } else {
                    blockId = this.layout.getBlockId(predecessorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else if (readBuffers.isEmpty()) {
                return CacheLayout.NO_ADDRESS;
            } else {
                // Found a bad pointer.
                throw new CacheCorruptedException(String.format("Buffer %s, Block %s: Unallocated.", this.id, blockId));
            }
        }

        return CacheLayout.NO_ADDRESS;
    }

    /**
     * Deletes the given Buffer-Block and all blocks it points to.
     *
     * @param blockId The id of the Buffer-Block to begin deleting at.
     * @return A {@link DeleteResult} containing the result of the deletion.
     */
    synchronized DeleteResult delete(int blockId) {
        validateBlockId(blockId, false);
        ByteBuf metadataBuf = getBlockBuffer(0);
        int deletedLength = 0;
        int predecessorAddress = CacheLayout.NO_ADDRESS;
        Stack<Integer> freedBlocks = new Stack<>(); // We're traversing backwards, but need these later in ascending order.
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            long blockMetadata = metadataBuf.getLong(blockId * this.layout.blockMetadataSize());
            if (this.layout.isUsedBlock(blockMetadata)) {
                // Clear metadata.
                freedBlocks.push(blockId);

                // Find predecessor, if any.
                predecessorAddress = this.layout.getPredecessorAddress(blockMetadata);
                deletedLength += this.layout.getLength(blockMetadata);
                if (predecessorAddress == CacheLayout.NO_ADDRESS || this.layout.getBufferId(predecessorAddress) != this.id) {
                    break;
                } else {
                    blockId = this.layout.getBlockId(predecessorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else {
                blockId = CacheLayout.NO_BLOCK_ID;
            }
        }

        deallocateBlocks(freedBlocks, metadataBuf);
        return new DeleteResult(deletedLength, predecessorAddress);
    }

    /**
     * Rolls back a partially executed call to {@link #write} that failed due to external causes (i.e., an InputStream
     * read error). This walks back the chain of blocks that were written, marks them as free and re-chains them into
     * the free block chain. This method is a simpler version of {@link #deallocateBlocks} that does not attempt to
     * merge two sorted lists and does not require the root metadata to have been properly updated (since it likely wasn't).
     * @param lastWrittenBlockId The id of the last Block id that has been written and had its metadata updated.
     * @param nextFreeBlockId    The id of the next free Block id after `lastWrittenBlockId`. Usually this is the id of
     *                           the block for which the write failed but for which we couldn't update the metadata yet.
     */
    @GuardedBy("this")
    private void rollbackWrite(int lastWrittenBlockId, int nextFreeBlockId) {
        ByteBuf metadataBuf = getBlockBuffer(0);
        int blockId = lastWrittenBlockId;
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            // Get block metadata and fetch the predecessor address, before we reset it.
            int bufIndex = blockId * this.layout.blockMetadataSize();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            int predecessorAddress = this.layout.getPredecessorAddress(blockMetadata);

            // Reset the block metadata.
            blockMetadata = this.layout.setNextFreeBlockId(this.layout.emptyBlockMetadata(), nextFreeBlockId);
            metadataBuf.setLong(bufIndex, blockMetadata);
            nextFreeBlockId = blockId;
            this.usedBlockCount--;

            // Determine the previous block to rollback.
            blockId = this.layout.getBlockId(predecessorAddress);
            if (this.layout.getBufferId(predecessorAddress) != this.id) {
                break;
            }
        }
    }

    /**
     * Marks the given Buffer-Blocks as free and makes them available for re-writing.
     *
     * @param blocks      A {@link Stack} of Buffer-Block ids. When invoking {@link Stack#pop()}, the blocks should be
     *                    returned in ascending order.
     * @param metadataBuf A {@link ByteBuf} representing the metadata buffer.
     */
    @GuardedBy("this")
    private void deallocateBlocks(Stack<Integer> blocks, ByteBuf metadataBuf) {
        if (blocks.size() == 0) {
            return;
        }

        // Find the highest empty block that is before the first block (P).
        int freedBlockId = blocks.pop();
        int prevBlockId = freedBlockId;
        long prevMetadata;
        do {
            prevBlockId--;
            prevMetadata = metadataBuf.getLong(prevBlockId * this.layout.blockMetadataSize());
        } while (prevBlockId > 0 && this.layout.isUsedBlock(prevMetadata));

        // Loop as long as there are no more freed blocks (F). At each iteration:
        // If P.Next < F, update F.next=P.next, P.next=F
        while (freedBlockId != CacheLayout.NO_BLOCK_ID) {
            // If the current free block (prevBlockId) has a free successor that is smaller than freedBlockId, then
            // follow the free block list until we find a successor that is after freedBlockId or reach the end.
            int prevNextFreeBlockId = this.layout.getNextFreeBlockId(prevMetadata);
            while (prevNextFreeBlockId != CacheLayout.NO_BLOCK_ID && prevNextFreeBlockId < freedBlockId) {
                prevBlockId = prevNextFreeBlockId;
                prevMetadata = metadataBuf.getLong(prevBlockId * this.layout.blockMetadataSize());
                prevNextFreeBlockId = this.layout.getNextFreeBlockId(prevMetadata);
            }

            // Point our newly freed block to this previous block's next free block id.
            long blockMetadata = this.layout.setNextFreeBlockId(this.layout.emptyBlockMetadata(), prevNextFreeBlockId);

            // Point this previous block to our newly freed block as the next free block in the chain.
            prevMetadata = this.layout.setNextFreeBlockId(prevMetadata, freedBlockId);
            metadataBuf.setLong(prevBlockId * this.layout.blockMetadataSize(), prevMetadata);
            metadataBuf.setLong(freedBlockId * this.layout.blockMetadataSize(), blockMetadata);

            freedBlockId = blocks.size() == 0 ? CacheLayout.NO_BLOCK_ID : blocks.pop();
            this.usedBlockCount--;
        }
    }

    /**
     * Writes the given data to the given buffer.
     *
     * @param blockBuffer A {@link ByteBuf} representing the Buffer-Block to write to.
     * @param bufferIndex The index within `blockBuffer` to begin writing at.
     * @param data        An {@link InputStream} representing the data to write.
     * @param blockLength The length of the block.
     * @return The number of bytes written.
     * @throws IOException If an exception occurred.
     */
    private int writeBlock(ByteBuf blockBuffer, int bufferIndex, InputStream data, int blockLength) throws IOException {
        blockBuffer.writerIndex(bufferIndex);
        int count = 0;
        while (count < blockLength) {
            int n = blockBuffer.writeBytes(data, blockLength);
            if (n < 0) {
                throw new EOFException();
            }
            count += n;
        }
        return count;
    }

    @GuardedBy("this")
    private ByteBuf getBlockBuffer(int blockIndex) {
        return getBuf().slice(blockIndex * this.layout.blockSize(), this.layout.blockSize());
    }

    @GuardedBy("this")
    private ByteBuf getBuf() {
        if (this.buf == null) {
            Exceptions.checkNotClosed(this.usedBlockCount < 0, this);
            assert this.usedBlockCount == 1;

            // TODO: this can also be changed to wrap a ByteBuffer or use ByteBuffer altogether.
            this.buf = this.allocator.directBuffer(this.layout.bufferSize(), this.layout.bufferSize());
            this.buf.setZero(0, this.buf.capacity()); // Clear out the buffer as Netty does not do it for us.

            // Format metadata.
            ByteBuf metadataBuf = getBlockBuffer(0);
            metadataBuf.writerIndex(0);
            for (int blockId = 0; blockId < this.layout.blocksPerBuffer(); blockId++) {
                long m = this.layout.emptyBlockMetadata();
                m = this.layout.setNextFreeBlockId(m, blockId == this.layout.blocksPerBuffer() - 1 ? CacheLayout.NO_BLOCK_ID : blockId + 1);
                metadataBuf.writeLong(m);
            }
        }

        return this.buf;
    }

    @GuardedBy("this")
    private void validateBlockId(int blockId, boolean canBeEmpty) {
        Preconditions.checkState(canBeEmpty || this.usedBlockCount > 1, "Empty buffer.");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer(),
                "blockId must be a number in the interval [1, %s).", this.layout.blocksPerBuffer());
    }

    //endregion

    //region Result Classes

    /**
     * Result from {@link #delete}.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class DeleteResult {
        /**
         * Number of bytes freed.
         */
        private final int deletedLength;
        /**
         * The address of the next Block-Buffer in the chain (from the last Block-Buffer that was deleted).
         */
        private final int predecessorAddress;

        @Override
        public String toString() {
            return String.format("DeletedLength = %d, Previous = %s", this.deletedLength, layout.getAddressString(predecessorAddress));
        }
    }

    /**
     * Result from {@link #write}.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class WriteResult {
        /**
         * The number of bytes remaining to be written after this Buffer.
         */
        private final int remainingLength;
        /**
         * The address of the last Block written to in this Buffer.
         */
        private final int lastBlockAddress;
        /**
         * The if of the first Block written to in this Buffer. Used for testing only.
         */
        @VisibleForTesting
        private final int firstBlockId;

        @Override
        public String toString() {
            return String.format("FirstBlockId=%d, LastBlockId=%d, RemainingLength=%d",
                    this.firstBlockId, layout.getBlockId(this.lastBlockAddress), this.remainingLength);
        }
    }

    //endregion
}

