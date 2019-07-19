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

/**
 * Base class that defines the memory layout for a block-based {@link CacheStorage}.
 */
abstract class CacheLayout {
    //region Members

    /**
     * The maximum number of bytes that can be stored in a {@link CacheStorage} using this type of layout.
     */
    static final long MAX_TOTAL_SIZE = 64 * 1024 * 1024 * 1024L;
    /**
     * The maximum size (in bytes) of any {@link CacheStorage} entry.
     */
    static final int MAX_ENTRY_SIZE = 0x03FF_FFFF; // 28 bits = 256MB
    /**
     * Null (inexistent) address.
     */
    static final int NO_ADDRESS = 0; // Valid addresses have cannot be 0 since Block 0 is reserved.
    /**
     * Null (inexistent) Buffer-Block id.
     */
    static final int NO_BLOCK_ID = 0; // 0 is the same as Metadata Block Id, so it's OK to use it.
    private final int maxBufferCount;
    private final int blocksPerBuffer;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link CacheLayout} class and performs any necessary sanity checks.
     */
    CacheLayout() {
        Preconditions.checkState(MAX_TOTAL_SIZE % bufferSize() == 0,
                "MAX_TOTAL_SIZE (%s) must be a multiple of bufferSize()(%s).", MAX_TOTAL_SIZE, bufferSize());
        this.maxBufferCount = (int) (MAX_TOTAL_SIZE / bufferSize());

        Preconditions.checkState(bufferSize() % blockSize() == 0,
                "bufferSize() (%s) must be a multiple of blockSize()(%s).", bufferSize(), blockSize());
        this.blocksPerBuffer = bufferSize() / blockSize();

        Preconditions.checkState(this.blocksPerBuffer * blockMetadataSize() == blockSize(),
                "All block metadata must fit exactly into a single block.");
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the number of Buffers to use for this layout.
     *
     * @return The number of Buffers.
     */
    int maxBufferCount() {
        return this.maxBufferCount;
    }

    /**
     * Gets a value indicating how many Blocks should a Buffer be split into.
     *
     * @return The number of Blocks per Buffer.
     */
    int blocksPerBuffer() {
        return this.blocksPerBuffer;
    }

    /**
     * Gets a value indicating the size, in bytes, of a Buffer.
     *
     * @return The Buffer Size, in bytes.
     */
    abstract int bufferSize();

    /**
     * Gets a value indicating the size, in bytes, of a Block in a Buffer.
     *
     * @return The Buffer-Block size, in bytes.
     */
    abstract int blockSize();

    /**
     * Gets a value indicating the size, in bytes, of the metadata for each Buffer-Block.
     *
     * @return The size of the Buffer-Block metadata, in bytes.
     */
    abstract int blockMetadataSize();

    /**
     * Gets the Id of the Buffer from the given address.
     *
     * @param address The address.
     * @return The Buffer id. This result is undefined if `address` has not been generated using {@link #calculateAddress}.
     */
    abstract int getBufferId(int address);

    /**
     * Gets the Id of the Buffer-Block from the given address.
     *
     * @param address The address.
     * @return The Buffer-Block id. This result is undefined if `address` has not been generated using {@link #calculateAddress}.
     */
    abstract int getBlockId(int address);

    /**
     * Generates an address by composing the given Buffer Id and Buffer-Block id.
     *
     * @param bufferId The Id of the Buffer to incorporate in the address.
     * @param blockId  The Id of the Buffer-Block to incorporate in the address.
     * @return The address. The Buffer and Buffer-Block can be extracted using {@link #getBufferId} and {@link #getBlockId}.
     */
    abstract int calculateAddress(int bufferId, int blockId);

    /**
     * Updates the given Block Metadata to indicate that its associated Buffer-Block has a successor with the given address.
     *
     * @param blockMetadata         The Block Metadata to update.
     * @param successorBlockAddress The successor address to set.
     * @return The resulting block metadata having the successor address set. This value is undefined if `blockMetadata`
     * was not generated using one of the methods in this class or if `successorBlockAddress` was not generated using
     * {@link #calculateAddress}.
     */
    abstract long setSuccessorAddress(long blockMetadata, int successorBlockAddress);

    /**
     * Gets the successor address from the given block metadata.
     *
     * @param blockMetadata The block metadata to extract from.
     * @return The successor address. This value is undefined if `blockMetadata` was not generated using one of the methods
     * in this class or if {@link #isUsedBlock} returns false on `blockMetadata`.
     */
    abstract int getSuccessorAddress(long blockMetadata);

    /**
     * Updates the given block metadata to indicate its associated Buffer-Block has a specific length.
     * @param blockMetadata The block metadata to update.
     * @param length The length to set.
     * @return The resulting block metadata. This value is undefined if `blockMetadata` was not generated using one of
     * the methods in this class.
     */
    abstract long setLength(long blockMetadata, int length);

    /**
     * Gets the length of the Buffer-Block associated with the given block metadata.
     * @param blockMetadata The block metadata to get from.
     * @return The length. This value is undefined if `blockMetadata` was not generated using one of the methods in this class
     * or if {@link #isUsedBlock} returns false on `blockMetadata`.
     */
    abstract int getLength(long blockMetadata);

    /**
     * Updates the given block metadata for an unallocated Buffer-Block to point to the next next free (unallocated) Block-Buffer.
     * @param blockMetadata The block metadata to update.
     * @param nextFreeBlockId The Buffer-Block id of the next free block.
     * @return The resulting block metadata. This value is undefined if `blockMetadata` was not generated using one of
     * the methods in this class.
     */
    abstract long setNextFreeBlockId(long blockMetadata, int nextFreeBlockId);

    /**
     * Gets the next free block id from the given block metadata.
     * @param blockMetadata The block metadata to get from.
     * @return The next free block id. This value is undefined if `blockMetadata` was not generated using one of the methods
     * in this class or if {@link #isUsedBlock} returns true on `blockMetadata`.
     */
    abstract int getNextFreeBlockId(long blockMetadata);

    /**
     * Gets a value indicating whether the Buffer-Block associated with the given block metadata is used or not.
     * @param blockMetadata The block metadata to query.
     * @return True if used, false otherwise. This value is undefined if `blockMetadata` was not generated using one of
     * the methods in this class.
     */
    abstract boolean isUsedBlock(long blockMetadata);

    /**
     * Gets a value indicating whether the Buffer-Block associated with the given block metadata is the first one for
     * an entry.
     *
     * @param blockMetadata The block metadata to query.
     * @return True if first, false otherwise. This value is undefined if `blockMetadata` was not generated using one of
     * the methods in this class or if {@link #isUsedBlock} returns false on `blockMetadata`.
     */
    abstract boolean isFirstBlock(long blockMetadata);

    /**
     * Generates a new Buffer-Block Metadata having IsUsed set to true.
     * @param first True if this is the first Buffer-Block for an entry (overall, not in the current Buffer).
     * @param nextFreeBlockId The Id of the next unallocated Buffer-Block.
     * @param length The length of the data in this Buffer-Block.
     * @param successorAddress The address of the next block in the sequence.
     * @return A new block metadata.
     */
    abstract long newBlockMetadata(boolean first, int nextFreeBlockId, int length, int successorAddress);

    /**
     * Generates a new Buffer-Block metadata with no contents.
     * @return A new block metadata.
     */
    abstract long emptyBlockMetadata();

    /**
     * Converts the given address into a readable format (used for logging).
     * @param address The address to decode.
     * @return A String. This value is undefined if `address` was not generated using one of the methods in this class.
     */
    String getAddressString(int address) {
        return address == NO_ADDRESS ? "" : String.format("Buffer = %d, Block = %d", getBufferId(address), getBlockId(address));
    }

    //endregion

    //region Default Layout

    /**
     * {@link CacheLayout} with the following characteristics:
     * - 2MB Buffers
     * - 4KB block size
     * - 511 usable Buffer-Blocks per Buffer.
     *
     * Address layout (26 bits used out of 32)
     * - Bits 0-5: 0
     * - Bits 6-21: Buffer Id (Max 65536 Buffers, 16 bits)
     * - Bits 22-31: Block Id (Max 1024 blocks, 10 bits)
     *
     * Metadata Layout (8 Bytes)
     * - Bit 0: Used Flag.
     * - Bit 1: First Block of Object.
     * - Bits 2-7: Not used.
     * - Bits 8-17: Next Free Block Id (10 bits. NO_BLOCK_ID if Used=1)
     * - Bits 18-31: Block Length (up to 16383, 14 bits)
     * - Bits 32-63: Successor Address.
     */
    static class DefaultLayout extends CacheLayout {
        @VisibleForTesting
        static final int ADDRESS_BIT_COUNT = Integer.SIZE;
        @VisibleForTesting
        static final int BLOCK_LENGTH_BIT_COUNT = 14;
        @VisibleForTesting
        static final int BLOCK_ID_BIT_COUNT = 10;
        private static final int BUFFER_SIZE = 2 * 1024 * 1024;
        private static final int BLOCK_SIZE = 4 * 1024;
        private static final long USED_FLAG = 0x8000_0000_0000_0000L;
        private static final long FIRST_BLOCK_FLAG = 0x4000_0000_0000_0000L;
        private static final long EMPTY_BLOCK_METADATA = 0L; // Not used, not first, no length and no successor.
        private static final int BLOCK_LENGTH_MASK = 0x3FFF; // 14 Bits.
        private static final int NEXT_FREE_BLOCK_ID_SHIFT_BITS = BLOCK_LENGTH_BIT_COUNT + ADDRESS_BIT_COUNT;
        private static final long NEXT_FREE_BLOCK_ID_CLEAR_MASK = 0xFF00_3FFF_FFFF_FFFFL; // Clear 10 bits in middle
        private static final int BLOCK_ID_MASK = 0x3FF;

        @Override
        int bufferSize() {
            return BUFFER_SIZE;
        }

        @Override
        int blockSize() {
            return BLOCK_SIZE;
        }

        @Override
        int blockMetadataSize() {
            return Long.BYTES;
        }

        @Override
        int getBufferId(int address) {
            return address >> BLOCK_ID_BIT_COUNT;
        }

        @Override
        int getBlockId(int address) {
            return address & BLOCK_ID_MASK;
        }

        @Override
        int calculateAddress(int bufferId, int blockId) {
            assert bufferId >= 0 && bufferId < maxBufferCount();
            assert blockId >= 0 && blockId < blocksPerBuffer();
            return (bufferId << BLOCK_ID_BIT_COUNT) + blockId;
        }

        @Override
        long setSuccessorAddress(long blockMetadata, int successorBlockAddress) {
            return (blockMetadata & 0xFFFF_FFFF_0000_0000L) | successorBlockAddress;
        }

        @Override
        int getSuccessorAddress(long blockMetadata) {
            return (int) (blockMetadata & 0XFFFF_FFFF);
        }

        @Override
        long setLength(long blockMetadata, int length) {
            // Clear current length.
            blockMetadata &= ~(long) BLOCK_LENGTH_MASK << ADDRESS_BIT_COUNT;

            // Set new length.
            blockMetadata |= (long) (length & BLOCK_LENGTH_MASK) << ADDRESS_BIT_COUNT;
            return blockMetadata;
        }

        @Override
        int getLength(long blockMetadata) {
            return (int) ((blockMetadata >> ADDRESS_BIT_COUNT) & BLOCK_LENGTH_MASK);
        }

        @Override
        long setNextFreeBlockId(long blockMetadata, int nextFreeBlockId) {
            return (blockMetadata & NEXT_FREE_BLOCK_ID_CLEAR_MASK) | ((long) nextFreeBlockId << NEXT_FREE_BLOCK_ID_SHIFT_BITS);
        }

        @Override
        int getNextFreeBlockId(long blockMetadata) {
            return (int) ((blockMetadata >> NEXT_FREE_BLOCK_ID_SHIFT_BITS) & BLOCK_ID_MASK);
        }

        @Override
        boolean isUsedBlock(long blockMetadata) {
            return (blockMetadata & USED_FLAG) == USED_FLAG;
        }

        @Override
        boolean isFirstBlock(long blockMetadata) {
            return (blockMetadata & FIRST_BLOCK_FLAG) == FIRST_BLOCK_FLAG;
        }

        @Override
        long newBlockMetadata(boolean first, int nextFreeBlockId, int length, int successorAddress) {
            // If we write something to it, it's used.
            long result = USED_FLAG;

            // Set First Block flag.
            if (first) {
                result |= FIRST_BLOCK_FLAG;
            }

            // Write next Free Block Id.
            result |= ((long) nextFreeBlockId & BLOCK_ID_MASK) << NEXT_FREE_BLOCK_ID_SHIFT_BITS;

            // Write length.
            result |= ((long) length & BLOCK_LENGTH_MASK) << ADDRESS_BIT_COUNT;

            // Write successor address.
            result |= 0xFFFF_FFFFL & successorAddress;
            return result;
        }

        @Override
        long emptyBlockMetadata() {
            return EMPTY_BLOCK_METADATA;
        }
    }

    //endregion
}
