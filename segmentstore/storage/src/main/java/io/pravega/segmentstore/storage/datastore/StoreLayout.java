package io.pravega.segmentstore.storage.datastore;

import com.google.common.base.Preconditions;

abstract class StoreLayout {
    static final long MAX_TOTAL_SIZE = 64L * 1024 * 1024 * 1024;
    static final int MAX_ENTRY_LENGTH = 0x03FF_FFFF; // 28 bits = 256MB
    static final int NO_ADDRESS = 0x8000_0000; // Valid addresses have leading zeroes.
    static final int NO_BLOCK_ID = 0; // 0 is the same as Metadata Block Id, so it's OK to use it.
    private final int maxBufferCount;
    private final int blocksPerBuffer;

    StoreLayout() {
        Preconditions.checkState(MAX_TOTAL_SIZE % bufferSize() == 0,
                "MAX_TOTAL_SIZE (%s) must be a multiple of bufferSize()(%s).", MAX_TOTAL_SIZE, bufferSize());
        this.maxBufferCount = (int) (MAX_TOTAL_SIZE / bufferSize());

        Preconditions.checkState(bufferSize() % blockSize() == 0,
                "bufferSize() (%s) must be a multiple of blockSize()(%s).", bufferSize(), blockSize());
        this.blocksPerBuffer = bufferSize() / blockSize();
//
//        Preconditions.checkState(this.blocksPerBuffer * blockMetadataLength() == blockSize(),
//                "All block metadata must fit exactly into a single block.");
    }

    int maxBufferCount() {
        return this.maxBufferCount;
    }

    int blocksPerBuffer() {
        return this.blocksPerBuffer;
    }

    abstract int bufferSize();

    abstract int blockSize();

    abstract int blockMetadataLength();

    abstract int getBufferId(int address);

    abstract int getBlockId(int address);

    abstract int calculateAddress(int bufferId, int blockId);

    abstract long setSuccessorAddress(long blockMetadata, int successorBlockAddress);

    abstract int getSuccessorAddress(long blockMetadata);

    abstract int getRemainingLength(long blockMetadata);

    abstract boolean isUsedBlock(long blockMetadata);

    abstract long newBlockMetadata(boolean first, int remainingLength, int successorAddress);

    abstract long emptyBlockMetadata();

    String getAddressString(int address) {
        return address == NO_ADDRESS ? "" : String.format("Buffer = %d, Block = %d", getBufferId(address), getBlockId(address));
    }

    /**
     * Metadata Layout (8 Bytes)
     * - Bit 0: Used Flag
     * - Bit 1: First Block of Object,
     * - Bits 2,3: Reserved (not used)
     * - Bits 4-31: Remaining Length (including this block; 28 bits)
     * - Bits 32-63: Successor Address.
     *
     * Address layout (24 bits)
     * - Bits 0-6: 0
     * - Bits 7-22: Buffer Id (Max 65536 Buffers, 16 bits)
     * - Bits 23-31: Block Id (512 blocks, 9 bits)
     */
    static class DefaultLayout extends StoreLayout {
        private static final int BUFFER_SIZE = 1 * 1024 * 1024; // This should 2 ....
        private static final int BLOCK_SIZE = 4 * 1024;
        private static final long USED_FLAG = 0x8000_0000_0000_0000L;
        private static final long FIRST_BLOCK_FLAG = 0x4000_0000_0000_0000L;
        private static final long EMPTY_BLOCK_METADATA = 0L; // Not used, not first, no length and no successor.
        private static final int BLOCK_ID_BITS = 9;
        private static final int BLOCK_ID_MASK = 0x1FF;

        @Override
        int bufferSize() {
            return BUFFER_SIZE;
        }

        @Override
        int blockSize() {
            return BLOCK_SIZE;
        }

        @Override
        int blockMetadataLength() {
            return Long.BYTES;
        }

        @Override
        int getBufferId(int address) {
            return address >> BLOCK_ID_BITS;
        }

        @Override
        int getBlockId(int address) {
            return address & BLOCK_ID_MASK;
        }

        @Override
        int calculateAddress(int bufferId, int blockId) {
            assert bufferId >= 0 && bufferId < maxBufferCount();
            assert blockId >= 0 && blockId < blocksPerBuffer();
            return (bufferId << BLOCK_ID_BITS) + blockId;
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
        int getRemainingLength(long blockMetadata) {
            return (int) ((blockMetadata >> 32) & 0x0FFF_FFFF);
        }

        @Override
        boolean isUsedBlock(long blockMetadata) {
            return (blockMetadata & USED_FLAG) == USED_FLAG;
        }

        @Override
        long newBlockMetadata(boolean first, int remainingLength, int successorAddress) {
            // If we write something to it, it's used.
            long result = USED_FLAG;

            // Set First Block flag.
            if (first) {
                result |= FIRST_BLOCK_FLAG;
            }

            // Write length.
            result |= ((long) remainingLength & MAX_ENTRY_LENGTH) << 32;

            // Write successor address.
            result |= 0xFFFF_FFFFL & successorAddress;
            return result;
        }

        @Override
        long emptyBlockMetadata() {
            return EMPTY_BLOCK_METADATA;
        }
    }

    /**
     * Metadata Layout (4 Bytes)
     * - Bit 0: Used Flag
     * - Bit 1: First Block
     * - Bit 2: Last Block
     * - If Bit 2 is 0: Bits 7-32: Successor Address
     * - If Bit 2 is 1: Bits 21-32: Length Used
     *
     * Address layout (25 bits)
     * - Bits 0-6: 0
     * - Bits 7-22: Buffer Id (Max 65536 Buffers, 16 bits)
     * - Bits 23-31: Block Id (512 blocks, 9 bits)
     */
    static class NoRemainingLengthLayout extends StoreLayout {
        private static final int BUFFER_SIZE = 1 * 1024 * 1024;
        private static final int BLOCK_SIZE = 2 * 1024;
        private static final int USED_FLAG = 0x8000_0000;
        private static final int FIRST_BLOCK_FLAG = 0x4000_0000;
        private static final int LAST_BLOCK_FLAG = 0x2000_0000;
        private static final int EMPTY_BLOCK_METADATA = 0; // Not used, not first, no length and no successor.
        private static final int BLOCK_ID_BITS = 9;
        private static final int BLOCK_ID_MASK = 0x1FF;

        @Override
        int bufferSize() {
            return BUFFER_SIZE;
        }

        @Override
        int blockSize() {
            return BLOCK_SIZE;
        }

        @Override
        int blockMetadataLength() {
            return Integer.BYTES;
        }

        @Override
        int getBufferId(int address) {
            return address >> BLOCK_ID_BITS;
        }

        @Override
        int getBlockId(int address) {
            return address & BLOCK_ID_MASK;
        }

        @Override
        int calculateAddress(int bufferId, int blockId) {
            assert bufferId >= 0 && bufferId < maxBufferCount();
            assert blockId >= 0 && blockId < blocksPerBuffer();
            return (bufferId << BLOCK_ID_BITS) + blockId;
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
        int getRemainingLength(long blockMetadata) {
            return (int) ((blockMetadata >> 32) & 0x0FFF_FFFF);
        }

        @Override
        boolean isUsedBlock(long blockMetadata) {
            return (blockMetadata & USED_FLAG) == USED_FLAG;
        }

        @Override
        long newBlockMetadata(boolean first, int remainingLength, int successorAddress) {
            // If we write something to it, it's used.
            long result = USED_FLAG;

            // Set First Block flag.
            if (first) {
                result |= FIRST_BLOCK_FLAG;
            }

            // Write length.
            result |= ((long) remainingLength & MAX_ENTRY_LENGTH) << 32;

            // Write successor address.
            result |= 0xFFFF_FFFFL & successorAddress;
            return result;
        }

        @Override
        long emptyBlockMetadata() {
            return EMPTY_BLOCK_METADATA;
        }
    }
}
