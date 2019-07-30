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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.pravega.common.Exceptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@ThreadSafe
class DirectMemoryBuffer implements AutoCloseable {
    //region Members

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

    DirectMemoryBuffer(int bufferId, @NonNull ByteBufAllocator allocator, @NonNull CacheLayout layout) {
        Preconditions.checkArgument(bufferId >= 0 && bufferId < layout.maxBufferCount());

        this.allocator = allocator;
        this.layout = layout;
        this.id = bufferId;
        this.usedBlockCount = 1;
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

    synchronized int getUsedBlockCount() {
        return this.usedBlockCount;
    }

    synchronized boolean isAllocated() {
        return this.buf != null;
    }

    synchronized boolean hasCapacity() {
        return this.usedBlockCount > 0 && this.usedBlockCount < this.layout.blocksPerBuffer();
    }

    @Override
    public synchronized String toString() {
        return String.format("Id=%d, UsedBlockCount=%d", this.id, this.usedBlockCount);
    }

    //endregion

    //region Buffer Implementation

    synchronized WriteResult write(InputStream data, int length, boolean first) throws IOException {
        if (this.usedBlockCount >= this.layout.blocksPerBuffer()) {
            // Full
            return null;
        }

        ArrayList<Integer> writtenBlocks = new ArrayList<>();
        int lastBlockLength = length % this.layout.blockSize();
        if (lastBlockLength == 0 && length > 0) {
            lastBlockLength = this.layout.blockSize();
        }

        ByteBuf metadataBuf = getBlockBuffer(0);
        long blockMetadata = metadataBuf.getLong(0);
        int blockId = this.layout.getNextFreeBlockId(blockMetadata);
        assert blockId != CacheLayout.NO_BLOCK_ID;
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            blockMetadata = metadataBuf.getLong(blockId * this.layout.blockMetadataSize());
            assert !this.layout.isUsedBlock(blockMetadata);
            writtenBlocks.add(blockId);
            if (length > 0) {
                length -= writeBlock(getBlockBuffer(blockId), 0, data, Math.min(length, this.layout.blockSize()));
            }

            blockId = this.layout.getNextFreeBlockId(blockMetadata);
            if (length <= 0) {
                // We are done.
                break;
            }
        }

        // Update the metadata into the buffer, now that we know the successors as well.
        blockMetadata = metadataBuf.getLong(0);
        blockMetadata = this.layout.setNextFreeBlockId(blockMetadata, blockId);
        metadataBuf.setLong(0, blockMetadata);

        // Each modified metadata.
        for (int i = 0; i < writtenBlocks.size(); i++) {
            blockId = writtenBlocks.get(i);
            boolean firstBlock = first && i == 0;
            boolean last = i == writtenBlocks.size() - 1;
            int successorAddress = last ? CacheLayout.NO_ADDRESS : this.layout.calculateAddress(this.id, writtenBlocks.get(i + 1));
            int blockLength = last && length == 0 ? lastBlockLength : this.layout.blockSize();
            long metadata = this.layout.newBlockMetadata(firstBlock, CacheLayout.NO_BLOCK_ID, blockLength, successorAddress);
            metadataBuf.setLong(blockId * this.layout.blockMetadataSize(), metadata);
        }

        this.usedBlockCount += writtenBlocks.size();
        return new WriteResult(length,
                this.layout.calculateAddress(this.id, writtenBlocks.get(0)),
                this.layout.calculateAddress(this.id, writtenBlocks.get(writtenBlocks.size() - 1)));
    }

    synchronized AppendResult tryAppend(int blockId, int expectedLastBlockLength, InputStream data, int maxLength) throws IOException {
        // Follow the block Id chain and find the last block.
        // If not found, return 0 bytes appended and the address of the last block.
        // Otherwise fill up last block and return number of bytes appended and the address.

        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            int bufIndex = blockId * this.layout.blockMetadataSize();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            assert this.layout.isUsedBlock(blockMetadata);
            int blockLength = this.layout.getLength(blockMetadata);
            int successorAddress = this.layout.getSuccessorAddress(blockMetadata);
            if (successorAddress == CacheLayout.NO_ADDRESS) {
                // Found the last block. Append to it.
                if (blockLength != expectedLastBlockLength) {
                    throw new IncorrectCacheEntryLengthException(String.format(
                            "Incorrect last block length. Expected %s, given %s.", blockLength, expectedLastBlockLength));
                }

                maxLength = Math.min(maxLength, this.layout.blockSize() - blockLength);
                blockLength += writeBlock(getBlockBuffer(blockId), blockLength, data, maxLength);

                // Update metadata.
                blockMetadata = this.layout.setLength(blockMetadata, blockLength);
                metadataBuf.setLong(bufIndex, blockMetadata);
                return new AppendResult(blockLength - expectedLastBlockLength, CacheLayout.NO_ADDRESS);
            } else if (blockLength < this.layout.blockSize()) {
                throw new CacheCorruptedException(String.format("Buffer %s, Block %s: Non-full, non-terminal block. Length=%s, Successor={%s}.",
                        this.id, blockId, blockLength, this.layout.getAddressString(successorAddress)));
            } else if (this.layout.getBufferId(successorAddress) != this.id) {
                // The next block is in a different buffer. Return a pointer to it.
                return new AppendResult(0, successorAddress);
            } else {
                // The next block is in this buffer.
                blockId = this.layout.getBlockId(successorAddress);
                assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
            }
        }

        // Couldn't append anything.
        return new AppendResult(0, CacheLayout.NO_ADDRESS);
    }

    synchronized int read(int blockId, List<ByteBuf> readBuffers) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            int bufIndex = blockId * this.layout.blockMetadataSize();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            if (this.layout.isUsedBlock(blockMetadata)) {
                int blockLength = this.layout.getLength(blockMetadata);
                int successorAddress = this.layout.getSuccessorAddress(blockMetadata);
                if (successorAddress != CacheLayout.NO_ADDRESS && blockLength < this.layout.blockSize()) {
                    throw new CacheCorruptedException(String.format("Buffer %s, Block %s: Non-full, non-terminal block. Length=%s, Successor={%s}.",
                            this.id, blockId, blockLength, this.layout.getAddressString(successorAddress)));
                }

                readBuffers.add(getBlockBuffer(blockId).slice(0, Math.min(blockLength, this.layout.blockSize())).asReadOnly());
                if (successorAddress == CacheLayout.NO_ADDRESS || this.layout.getBufferId(successorAddress) != this.id) {
                    // We are done.
                    return successorAddress;
                } else {
                    blockId = this.layout.getBlockId(successorAddress);
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

    synchronized DeleteResult delete(int blockId) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        int deletedLength = 0;
        int successorAddress = CacheLayout.NO_ADDRESS;
        ArrayList<Integer> freedBlocks = new ArrayList<>();
        while (blockId != CacheLayout.NO_BLOCK_ID) {
            long blockMetadata = metadataBuf.getLong(blockId * this.layout.blockMetadataSize());
            if (this.layout.isUsedBlock(blockMetadata)) {
                // Clear metadata.
                freedBlocks.add(blockId);

                // Find successor, if any.
                successorAddress = this.layout.getSuccessorAddress(blockMetadata);
                deletedLength += this.layout.getLength(blockMetadata);
                if (successorAddress == CacheLayout.NO_ADDRESS || this.layout.getBufferId(successorAddress) != this.id) {
                    break;
                } else {
                    blockId = this.layout.getBlockId(successorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else {
                blockId = CacheLayout.NO_BLOCK_ID;
            }
        }

        deallocateBlocks(freedBlocks, metadataBuf);
        return new DeleteResult(deletedLength, successorAddress);
    }

    synchronized void setSuccessor(int blockAddress, int successorBlockAddress) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        int bufId = this.layout.getBufferId(blockAddress);
        int blockId = this.layout.getBlockId(blockAddress);
        Preconditions.checkArgument(this.id == bufId);
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());
        ByteBuf metadataBuf = getBlockBuffer(0);
        int bufIndex = blockId * this.layout.blockMetadataSize();
        long blockMetadata = metadataBuf.getLong(bufIndex);
        Preconditions.checkArgument(this.layout.isUsedBlock(blockMetadata));
        blockMetadata = this.layout.setSuccessorAddress(blockMetadata, successorBlockAddress);
        metadataBuf.setLong(bufIndex, blockMetadata);
    }

    @GuardedBy("this")
    private void deallocateBlocks(List<Integer> blocks, ByteBuf metadataBuf) {
        Iterator<Integer> iterator = blocks.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        // Find the highest empty block that is before the first block (P).
        int freedBlockId = iterator.next();
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

            freedBlockId = iterator.hasNext() ? iterator.next() : CacheLayout.NO_BLOCK_ID;
        }

        this.usedBlockCount -= blocks.size();
    }

    private int writeBlock(ByteBuf blockBuffer, int bufferIndex, InputStream data, int blockLength) throws IOException {
        blockBuffer.writerIndex(bufferIndex);
        int count = 0;
        while (count < blockLength) {
            int n = blockBuffer.writeBytes(data, blockLength);
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

            // TODO: this can also be changed to wrap a ByteBuffer.
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
        private final int successorAddress;

        @Override
        public String toString() {
            return String.format("DeletedLength = %d, Next = %s", this.deletedLength, layout.getAddressString(successorAddress));
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
         * The address of the first Block written to in this Buffer.
         */
        private final int firstBlockAddress;
        /**
         * The address of the last Block written to in this Buffer.
         */
        private final int lastBlockAddress;

        @Override
        public String toString() {
            return String.format("FirstBlockId=%d, LastBlockId=%d, RemainingLength=%d",
                    layout.getBlockId(this.firstBlockAddress), layout.getBlockId(this.lastBlockAddress), this.remainingLength);
        }
    }

    /**
     * Result from {@link #tryAppend}
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class AppendResult {
        /**
         * The number of bytes appended. This is 0 if {@link #getNextBlockAddress()} is {@link CacheLayout#NO_ADDRESS}.
         */
        private final int appendedLength;
        /**
         * The address of the next Block-Buffer in the chain for the entry to append. This value is undefined if
         * {@link #getAppendedLength()} is non-zero.
         */
        private final int nextBlockAddress;

        @Override
        public String toString() {
            return String.format("NextBlock={%s}, AppendedLength=%d", layout.getAddressString(this.nextBlockAddress), this.appendedLength);
        }
    }

    //endregion
}

