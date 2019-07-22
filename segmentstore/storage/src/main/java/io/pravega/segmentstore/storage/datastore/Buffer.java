package io.pravega.segmentstore.storage.datastore;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@ThreadSafe
class Buffer implements AutoCloseable {
    @Getter
    private final int id;
    private final StoreLayout layout;
    @GuardedBy("this")
    private final ByteBuf buf;
    @GuardedBy("this")
    private int usedBlockCount;

    Buffer(int bufferId, Function<Integer, ByteBuf> bufferCreator, @NonNull StoreLayout layout) {
        Preconditions.checkArgument(bufferId >= 0 && bufferId < layout.maxBufferCount());

        this.buf = bufferCreator.apply(layout.bufferSize());
        this.buf.setZero(0, this.buf.capacity()); // Clear out the buffer as Netty does not do it for us.
        this.layout = layout;
        this.id = bufferId;
        this.usedBlockCount = 1;
        formatMetadata();
    }

    private void formatMetadata() {
        ByteBuf metadataBuf = getBlockBuffer(0);
        metadataBuf.writerIndex(0);
        for (int blockId = 0; blockId < this.layout.blocksPerBuffer(); blockId++) {
            long m = this.layout.emptyBlockMetadata();
            m = this.layout.setNextFreeBlockId(m, blockId == this.layout.blocksPerBuffer() - 1 ? StoreLayout.NO_BLOCK_ID : blockId + 1);
            metadataBuf.writeLong(m);
        }
    }

    @Override
    public synchronized void close() {
        if (this.buf.refCnt() > 0) {
            this.buf.release();
        }
        this.usedBlockCount = -1;
    }

    synchronized int getUsedBlockCount() {
        return this.usedBlockCount;
    }

    synchronized boolean hasCapacity() {
        return this.usedBlockCount > 0 && this.usedBlockCount < this.layout.blocksPerBuffer();
    }

    @Override
    public synchronized String toString() {
        return String.format("Id=%d, UsedBlockCount=%d", this.id, this.usedBlockCount);
    }

    synchronized WriteResult write(InputStream data, int length, boolean first) throws IOException {
        if (this.usedBlockCount >= this.layout.blocksPerBuffer()) {
            // Full
            return null;
        }

        ArrayList<Integer> result = new ArrayList<>();
        int lastBlockLength = length % this.layout.blockSize();

        ByteBuf metadataBuf = getBlockBuffer(0);
        long blockMetadata = metadataBuf.getLong(0);
        int blockId = this.layout.getNextFreeBlockId(blockMetadata);
        assert blockId != StoreLayout.NO_BLOCK_ID;
        while (blockId != StoreLayout.NO_BLOCK_ID) {
            blockMetadata = metadataBuf.getLong(blockId * this.layout.blockMetadataLength());
            assert !this.layout.isUsedBlock(blockMetadata);
            result.add(blockId);
            int blockBytes = Math.min(length, this.layout.blockSize());
            if (length > 0) {
                ByteBuf blockBuffer = getBlockBuffer(blockId);
                blockBuffer.writerIndex(0);
                while (blockBytes > 0) {
                    int n = blockBuffer.writeBytes(data, blockBytes);
                        blockBytes -= n;
                    length -= n;
                }
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
        for (int i = 0; i < result.size(); i++) {
            blockId = result.get(i);
            boolean firstBlock = first && i == 0;
            boolean last = i == result.size() - 1;
            int successorAddress = last ? StoreLayout.NO_ADDRESS : this.layout.calculateAddress(this.id, result.get(i + 1));
            int blockLength = last && length == 0 ? lastBlockLength : this.layout.blockSize();
            long metadata = this.layout.newBlockMetadata(firstBlock, StoreLayout.NO_BLOCK_ID, blockLength, successorAddress);
            metadataBuf.setLong(blockId * this.layout.blockMetadataLength(), metadata);
        }

        this.usedBlockCount += result.size();
        return new WriteResult(length, this.layout.calculateAddress(this.id, result.get(0)), this.layout.calculateAddress(this.id, result.get(result.size() - 1)));
    }

    synchronized ReWriteResult write(int blockId, InputStream data, int length) {
        // Re-write beginning at the given block id, with the given length.
        // Overwrite all entries.
        // If remaining length ==0 and did not reach the last block, delete remaining blocks and return
        // If remaining length > 0 and reached the last block, perform normal write...
        return null;
    }

    synchronized AppendResult append(int blockId, int expectedLastBlockLength, InputStream data, int maxLength) throws IOException {
        // Follow the block Id chain and find the last block.
        // If not found, return 0 bytes appended and the address of the last block.
        // Otherwise fill up last block and return number of bytes appended and the address.

        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        while (blockId != StoreLayout.NO_BLOCK_ID) {
            int bufIndex = blockId * this.layout.blockMetadataLength();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            if (this.layout.isUsedBlock(blockMetadata)) {
                int blockLength = this.layout.getLength(blockMetadata);
                int successorAddress = this.layout.getSuccessorAddress(blockMetadata);
                if (successorAddress == StoreLayout.NO_ADDRESS) {
                    // Found the last block. Append to it.
                    Preconditions.checkArgument(blockLength == expectedLastBlockLength,
                            "Incorrect last block length. Expected %s, given %s.", blockLength, expectedLastBlockLength);

                    ByteBuf blockBuffer = getBlockBuffer(blockId);
                    blockBuffer.writerIndex(blockLength);
                    maxLength = Math.min(maxLength, this.layout.blockSize() - blockLength);
                    while (maxLength > 0) {
                        int n = blockBuffer.writeBytes(data, maxLength);
                        maxLength -= n;
                        blockLength += n;
                    }

                    // Update metadata.
                    blockMetadata = this.layout.setLength(blockMetadata, blockLength);
                    metadataBuf.setLong(bufIndex, blockMetadata);
                    return new AppendResult(blockLength - expectedLastBlockLength, StoreLayout.NO_ADDRESS);
                } else if (blockLength < this.layout.blockSize()) {
                    throw new RuntimeException(new Exception("corruption: non-full, non-terminal block."));
                } else if (this.layout.getBufferId(successorAddress) != this.id) {
                    // The next block is in a different buffer. Return a pointer to it.
                    return new AppendResult(0, successorAddress);
                } else {
                    // The next block is in this buffer.
                    blockId = this.layout.getBlockId(successorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else {
                // Found a bad pointer. TODO: appropriate exception (NotExists?) or just return nothing?
                throw new RuntimeException(new Exception("corruption: block id not used"));
            }
        }

        // Couldn't append anything.
        return new AppendResult(0, StoreLayout.NO_ADDRESS);
    }

    synchronized int read(int blockId, List<ByteBuf> readBuffers) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        while (blockId != StoreLayout.NO_BLOCK_ID) {
            int bufIndex = blockId * this.layout.blockMetadataLength();
            long blockMetadata = metadataBuf.getLong(bufIndex);
            if (this.layout.isUsedBlock(blockMetadata)) {
                int blockLength = this.layout.getLength(blockMetadata);
                int successorAddress = this.layout.getSuccessorAddress(blockMetadata);
                if (successorAddress != StoreLayout.NO_ADDRESS && blockLength < this.layout.blockSize()) {
                    throw new RuntimeException(new Exception("corruption: non-full, non-terminal block."));
                }

                readBuffers.add(getBlockBuffer(blockId).slice(0, Math.min(blockLength, this.layout.blockSize())).asReadOnly());
                if (successorAddress == StoreLayout.NO_ADDRESS || this.layout.getBufferId(successorAddress) != this.id) {
                    // We are done.
                    return successorAddress;
                } else {
                    blockId = this.layout.getBlockId(successorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else if (readBuffers.isEmpty()) {
                return StoreLayout.NO_ADDRESS;
            } else {
                // Found a bad pointer.
                throw new RuntimeException(new Exception("corruption"));
            }
        }

        return StoreLayout.NO_ADDRESS;
    }

    synchronized DeleteResult delete(int blockId) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());

        ByteBuf metadataBuf = getBlockBuffer(0);
        int deletedLength = 0;
        int successorAddress = StoreLayout.NO_ADDRESS;
        ArrayList<Integer> freedBlocks = new ArrayList<>();
        while (blockId != StoreLayout.NO_BLOCK_ID) {
            long blockMetadata = metadataBuf.getLong(blockId * this.layout.blockMetadataLength());
            if (this.layout.isUsedBlock(blockMetadata)) {
                // Clear metadata.
                freedBlocks.add(blockId);

                // Find successor, if any.
                successorAddress = this.layout.getSuccessorAddress(blockMetadata);
                deletedLength += this.layout.getLength(blockMetadata);
                if (successorAddress == StoreLayout.NO_ADDRESS || this.layout.getBufferId(successorAddress) != this.id) {
                    break;
                } else {
                    blockId = this.layout.getBlockId(successorAddress);
                    assert blockId >= 1 && blockId < this.layout.blocksPerBuffer();
                }
            } else {
                blockId = StoreLayout.NO_BLOCK_ID;
            }
        }

        deallocateBlocks(freedBlocks, metadataBuf);
        return new DeleteResult(deletedLength, successorAddress);
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
            prevMetadata = metadataBuf.getLong(prevBlockId * this.layout.blockMetadataLength());
        } while (prevBlockId > 0 && this.layout.isUsedBlock(prevMetadata));

        // Loop as long as there are no more freed blocks (F). At each iteration:
        // If P.Next < F, update F.next=P.next, P.next=F
        while (freedBlockId != StoreLayout.NO_BLOCK_ID) {
            // If the current free block (prevBlockId) has a free successor that is smaller than freedBlockId, then
            // follow the free block list until we find a successor that is after freedBlockId or reach the end.
            int prevNextFreeBlockId = this.layout.getNextFreeBlockId(prevMetadata);
            while (prevNextFreeBlockId != StoreLayout.NO_BLOCK_ID && prevNextFreeBlockId < freedBlockId) {
                prevBlockId = prevNextFreeBlockId;
                prevMetadata = metadataBuf.getLong(prevBlockId * this.layout.blockMetadataLength());
                prevNextFreeBlockId = this.layout.getNextFreeBlockId(prevMetadata);
            }

            // Point our newly freed block to this previous block's next free block id.
            long blockMetadata = this.layout.setNextFreeBlockId(this.layout.emptyBlockMetadata(), prevNextFreeBlockId);

            // Point this previous block to our newly freed block as the next free block in the chain.
            prevMetadata = this.layout.setNextFreeBlockId(prevMetadata, freedBlockId);
            metadataBuf.setLong(prevBlockId * this.layout.blockMetadataLength(), prevMetadata);
            metadataBuf.setLong(freedBlockId * this.layout.blockMetadataLength(), blockMetadata);

            freedBlockId = iterator.hasNext() ? iterator.next() : StoreLayout.NO_BLOCK_ID;
        }

        this.usedBlockCount -= blocks.size();
    }

    synchronized void setSuccessor(int blockAddress, int successorBlockAddress) {
        Preconditions.checkState(this.usedBlockCount > 1, "empty");
        int bufId = this.layout.getBufferId(blockAddress);
        int blockId = this.layout.getBlockId(blockAddress);
        Preconditions.checkArgument(this.id == bufId);
        Preconditions.checkArgument(blockId >= 1 && blockId < this.layout.blocksPerBuffer());
        ByteBuf metadataBuf = getBlockBuffer(0);
        int bufIndex = blockId * this.layout.blockMetadataLength();
        long blockMetadata = metadataBuf.getLong(bufIndex);
        Preconditions.checkArgument(this.layout.isUsedBlock(blockMetadata));
        blockMetadata = this.layout.setSuccessorAddress(blockMetadata, successorBlockAddress);
        metadataBuf.setLong(bufIndex, blockMetadata);
    }

    @GuardedBy("this")
    private ByteBuf getBlockBuffer(int blockIndex) {
        return this.buf.slice(blockIndex * this.layout.blockSize(), this.layout.blockSize());
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class DeleteResult {
        private final int deletedLength;
        private final int successorAddress;

        @Override
        public String toString() {
            return String.format("DeletedLength = %d, Next = {}", this.deletedLength, layout.getAddressString(successorAddress));
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class WriteResult {
        private final int remainingLength;
        private final int firstBlockAddress;
        private final int lastBlockAddress;

        @Override
        public String toString() {
            return String.format("FirstBlockId=%d, LastBlockId=%d, RemainingLength=%d",
                    layout.getBlockId(this.firstBlockAddress), layout.getBlockId(this.lastBlockAddress), this.remainingLength);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class ReWriteResult {
        private final int remainingLength;
        private final int lastBlockAddress;

        @Override
        public String toString() {
            return String.format("LastBlockId=%d, RemainingLength=%d", layout.getBlockId(this.lastBlockAddress), this.remainingLength);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    class AppendResult {
        private final int appendedlength;
        private final int nextBlockAddress;

        @Override
        public String toString() {
            return String.format("NextBlock={%s}, AppendedLength=%d", layout.getAddressString(this.nextBlockAddress), this.appendedlength);
        }
    }
}

