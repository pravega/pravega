package io.pravega.segmentstore.storage.datastore;

import com.google.common.base.Preconditions;

abstract class StoreLayout {
    static final long MAX_TOTAL_SIZE = 64 * 1024 * 1024 * 1024L;
    static final int MAX_ENTRY_LENGTH = 0x03FF_FFFF; // 28 bits = 256MB
    static final int NO_ADDRESS = 0; // Valid addresses have cannot be 0 since Block 0 is reserved.
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

    abstract int getLength(long blockMetadata);

    abstract long setNextFreeBlockId(long blockMetadata, int nextFreeBlockId);

    abstract int getNextFreeBlockId(long blockMetadata);

    abstract boolean isUsedBlock(long blockMetadata);

    abstract long newBlockMetadata(boolean first, int nextFreeBlockId, int length, int successorAddress);

    abstract long emptyBlockMetadata();

    String getAddressString(int address) {
        return address == NO_ADDRESS ? "" : String.format("Buffer = %d, Block = %d", getBufferId(address), getBlockId(address));
    }

    /**
     * Metadata Layout (8 Bytes)
     * - Bit 0: Used Flag.
     * - Bit 1: First Block of Object.
     * - Bits 2-7: Not used.
     * - Bits 8-17: Next Free Block Id (10 bits. NO_BLOCK_ID if Used=1)
     * - Bits 18-31: Block Length (up to 16383, 14 bits)
     * - Bits 32-63: Successor Address.
     *
     * Address layout (24 bits used out of 32)
     * - Bits 0-7: 0
     * - Bits 8-23: Buffer Id (Max 65536 Buffers, 16 bits)
     * - Bits 24-31: Block Id (256 blocks, 8 bits)
     */
    static class DefaultLayout extends StoreLayout {
        private static final int BUFFER_SIZE = 1 * 1024 * 1024; // This should be 2 ....
        private static final int BLOCK_SIZE = 4 * 1024;
        private static final long USED_FLAG = 0x8000_0000_0000_0000L;
        private static final long FIRST_BLOCK_FLAG = 0x4000_0000_0000_0000L;
        private static final long EMPTY_BLOCK_METADATA = 0L; // Not used, not first, no length and no successor.

        private static final int BLOCK_LENGTH_BIT_COUNT = 14;
        private static final int ADDRESS_BIT_COUNT = Integer.SIZE;
        private static final int NEXT_FREE_BLOCK_ID_SHIFT_BITS = BLOCK_LENGTH_BIT_COUNT + ADDRESS_BIT_COUNT;
        private static final int NEXT_FREE_BLOCK_ID_MASK = 0x3FF; // 10 bits.
        private static final long NEXT_FREE_BLOCK_ID_CLEAR_MASK = 0xFF00_3FFF_FFFF_FFFFL; // Clear 10 bits in middle
        private static final int BLOCK_ID_BIT_COUNT = 8;
        private static final int BLOCK_ID_MASK = 0xFF;

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
        int getLength(long blockMetadata) {
            return (int) ((blockMetadata >> ADDRESS_BIT_COUNT) & 0x0FFF_FFFF);
        }

        @Override
        long setNextFreeBlockId(long blockMetadata, int nextFreeBlockId) {
            return (blockMetadata & NEXT_FREE_BLOCK_ID_CLEAR_MASK) | ((long) nextFreeBlockId << NEXT_FREE_BLOCK_ID_SHIFT_BITS);
        }

        @Override
        int getNextFreeBlockId(long blockMetadata) {
            return (int) ((blockMetadata >> NEXT_FREE_BLOCK_ID_SHIFT_BITS) & NEXT_FREE_BLOCK_ID_MASK);
        }

        @Override
        boolean isUsedBlock(long blockMetadata) {
            return (blockMetadata & USED_FLAG) == USED_FLAG;
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
            result |= ((long) length & MAX_ENTRY_LENGTH) << ADDRESS_BIT_COUNT;

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
