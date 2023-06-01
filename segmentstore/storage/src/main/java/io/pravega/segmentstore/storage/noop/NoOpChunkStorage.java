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
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.segmentstore.storage.chunklayer.InvalidOffsetException;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * NoOp implementation.
 */
@Slf4j
public class NoOpChunkStorage extends AbstractInMemoryChunkStorage {
    @Getter
    @Setter
    ConcurrentHashMap<String, ChunkData> chunkMetadata = new ConcurrentHashMap<>();

    public NoOpChunkStorage(Executor executor) {
        super(executor);
    }

    @Override
    public boolean supportsDataIntegrityCheck() {
        return false;
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorage::doGetInfo");
        }
        return ChunkInfo.builder().name(chunkName).length(chunkData.length).build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null != chunkData) {
            throw new ChunkAlreadyExistsException(chunkName, "NoOpChunkStorage::doCreate");
        }
        chunkMetadata.put(chunkName, new ChunkData());
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        return chunkMetadata.containsKey(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorage::doDelete");
        }
        if (chunkData.isReadonly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        chunkMetadata.remove(handle.getChunkName());
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorage::doOpenRead");
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorage::doOpenWrite");
        }
        return new ChunkHandle(chunkName, chunkData.isReadonly);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorage::doRead");
        }
        if (fromOffset >= chunkData.length) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of chunk (%d).", fromOffset, chunkData.length));
        }
        if (fromOffset + length > chunkData.length) {
            throw new IndexOutOfBoundsException("fromOffset");
        }
        if (fromOffset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    fromOffset, bufferOffset, length, buffer.length));
        }

        return length;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException {

        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorage::doWrite");
        }
        if (chunkData.isReadonly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        if (offset != chunkData.length) {
            throw new InvalidOffsetException(handle.getChunkName(), chunkData.length, offset, "doWrite");
        }
        // Consume data
        try {
            data.readNBytes(length);
        } catch (Exception e) {
            throw new ChunkStorageException(handle.getChunkName(), "Unexpected exception while consuming data", e);
        }
        chunkData.length = offset + length;
        chunkMetadata.put(handle.getChunkName(), chunkData);

        return length;
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        if (!supportsConcat()) {
            throw new UnsupportedOperationException("Chunk storage does not support doConcat");
        }
        int total = 0;
        for (ConcatArgument chunk : chunks) {
            val chunkData = chunkMetadata.get(chunk.getName());
            if (null == chunkData) {
                throw new ChunkNotFoundException(chunk.getName(), "NoOpChunkStorage::doConcat");
            }
            Preconditions.checkState(chunkData.length >= chunk.getLength());
            total += chunk.getLength();
        }

        val targetChunkData = chunkMetadata.get(chunks[0].getName());
        targetChunkData.length = total;

        return total;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException {
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new ChunkNotFoundException(handle.getChunkName(), "NoOpChunkStorage::doTruncate");
        }
        if (offset < chunkData.length) {
            chunkData.length = offset;
            return true;
        }
        return false;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        String chunkName = handle.getChunkName();
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new ChunkNotFoundException(chunkName, "NoOpChunkStorage::doSetReadOnly");
        }
        chunkData.isReadonly = isReadOnly;
    }

    @Override
    public void addChunk(String chunkName, long length) {
        ChunkData chunkData = new ChunkData();
        chunkData.length = length;
        chunkMetadata.put(chunkName, chunkData);
    }

    /**
     * Stores the chunk data.
     */
    private static class ChunkData {
        private long length;
        private boolean isReadonly;
    }
}
