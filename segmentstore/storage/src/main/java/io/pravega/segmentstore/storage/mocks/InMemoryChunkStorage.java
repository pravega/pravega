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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.segmentstore.storage.chunklayer.InvalidOffsetException;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * In-Memory mock for ChunkStorage. Contents is destroyed when object is garbage collected.
 */
@Slf4j
public class InMemoryChunkStorage extends AbstractInMemoryChunkStorage {
    private final ConcurrentHashMap<String, InMemoryChunk> chunks = new ConcurrentHashMap<String, InMemoryChunk>();

    public InMemoryChunkStorage(Executor executor) {
        super(executor);
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        InMemoryChunk chunk = chunks.get(chunkName);
        if (null == chunk) {
            throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorage::doGetInfo");
        }
        return ChunkInfo.builder()
                .length(chunk.getLength())
                .name(chunkName)
                .build();
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        Preconditions.checkNotNull(chunkName);
        if (null != chunks.putIfAbsent(chunkName, new InMemoryChunk(chunkName))) {
            throw new ChunkAlreadyExistsException(chunkName, "InMemoryChunkStorage::doCreate");
        }
        ChunkHandle handle = new ChunkHandle(chunkName, false);
        int bytesWritten = doWriteInternal(handle, 0, length, data);
        if (bytesWritten < length) {
            doDelete(ChunkHandle.writeHandle(chunkName));
            throw new ChunkStorageException(chunkName, "doCreateWithContent - invalid length returned");
        }
        return handle;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        if (!supportsAppend()) {
            throw new UnsupportedOperationException("Attempt to create empty object when append is not supported.");
        }

        if (null != chunks.putIfAbsent(chunkName, new InMemoryChunk(chunkName))) {
            throw new ChunkAlreadyExistsException(chunkName, "InMemoryChunkStorage::doCreate");
        }
        return new ChunkHandle(chunkName, false);
    }

    @Override
    protected boolean checkExists(String chunkName) {
        return chunks.containsKey(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        if (null == chunk) {
            throw new ChunkNotFoundException(handle.getChunkName(), "InMemoryChunkStorage::doDelete");
        }
        if (chunk.isReadOnly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        chunks.remove(handle.getChunkName());
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        if (chunks.containsKey(chunkName)) {
            return new ChunkHandle(chunkName, true);
        }
        throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorage::doOpenRead");
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(chunkName);
        if (null != chunk) {
            return new ChunkHandle(chunkName, chunk.isReadOnly);
        }
        throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorage::doOpenWrite");
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        if (fromOffset >= chunk.getLength()) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of chunk (%d).", fromOffset, chunk.getLength()));
        }
        if (length == 0) {
            throw new ChunkStorageException(handle.getChunkName(), "Attempt to read 0 bytes");
        }

        // This is implemented this way to simulate possibility of partial read.
        InMemoryChunkData matchingData = null;

        // Find chunk that contains data.
        int floorIndex = CollectionHelpers.findGreatestLowerBound(chunk.inMemoryChunkDataList,
                chunkData -> Long.compare(fromOffset, chunkData.start));
        if (floorIndex != -1) {
            matchingData = chunk.inMemoryChunkDataList.get(floorIndex);
        }

        if (null != matchingData) {
            int startIndex = (int) (fromOffset - matchingData.start);
            byte[] source = matchingData.getData();
            int i;
            for (i = 0; i < length && bufferOffset + i < buffer.length && startIndex + i < source.length; i++) {
                buffer[bufferOffset + i] = source[startIndex + i];
            }
            return i;
        }
        throw new ChunkStorageException(handle.getChunkName(), "No data was read");
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        if (!supportsAppend()) {
            throw new UnsupportedOperationException("Attempt to create empty object when append is not supported.");
        }

        return doWriteInternal(handle, offset, length, data);
    }

    private int doWriteInternal(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        long oldLength = chunk.getLength();
        if (chunk.isReadOnly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        if (offset != chunk.getLength()) {
            throw new InvalidOffsetException(handle.getChunkName(), chunk.getLength(), offset, "doWrite");
        }
        if (length == 0) {
            return 0;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(length);
        byte[] bytes = new byte[length];
        int totalBytesRead = 0;
        int bytesRead = 0;
        try {
            while ((bytesRead = data.read(bytes)) != -1) {
                out.write(bytes, 0, bytesRead);
                totalBytesRead += bytesRead;
            }
        } catch (IOException e) {
            throw new ChunkStorageException(handle.getChunkName(), "Error while reading", e);
        }
        Preconditions.checkState(length == totalBytesRead, "%s %s", length, totalBytesRead);

        byte[] writtenBytes = out.toByteArray();
        Preconditions.checkState(writtenBytes.length == totalBytesRead);
        chunk.append(writtenBytes);
        Preconditions.checkState(oldLength + totalBytesRead == chunk.getLength());
        return totalBytesRead;
    }

    private InMemoryChunk getInMemoryChunk(ChunkHandle handle) throws ChunkStorageException {
        String chunkName = handle.getChunkName();
        return getInMemoryChunk(chunkName);
    }

    private InMemoryChunk getInMemoryChunk(String chunkName) throws ChunkNotFoundException {
        InMemoryChunk retValue = chunks.get(chunkName);
        if (null == retValue) {
            throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorage::getInMemoryChunk");
        }
        return retValue;
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        return concatAtExactLengths(chunks);
    }

    private int concatAtExactLengths(ConcatArgument[] chunks) throws ChunkStorageException {
        int total = Math.toIntExact(chunks[0].getLength());
        final InMemoryChunk targetChunk = getInMemoryChunk(chunks[0].getName());
        synchronized (targetChunk) {
            targetChunk.truncate(chunks[0].getLength());
            for (int i = 1; i < chunks.length; i++) {
                ConcatArgument source = chunks[i];
                InMemoryChunk chunk = getInMemoryChunk(source.getName());
                chunk.truncate(chunks[i].getLength());
                for (InMemoryChunkData inMemoryChunkData : chunk.getInMemoryChunkDataList()) {
                    targetChunk.append(inMemoryChunkData.getData());
                }
                total += chunks[i].getLength();
            }
            return total;
        }
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        return chunk.truncate(offset);
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        chunk.setReadOnly(isReadOnly);
    }

    @Override
    public void addChunk(String chunkName, long length) {
        InMemoryChunk inMemoryChunk = new InMemoryChunk(chunkName);
        inMemoryChunk.append(new byte[Math.toIntExact(length)]);
        chunks.put(chunkName, inMemoryChunk);
    }

    /**
     * In memory chunk.
     */
    static class InMemoryChunk {
        @Getter
        @Setter
        private long length;

        @Getter
        @Setter
        private String name;

        @Getter
        @Setter
        private boolean isReadOnly;

        @Getter
        private final List<InMemoryChunkData> inMemoryChunkDataList = new ArrayList<>();

        public InMemoryChunk(String name) {
            this.name = name;
        }

        public void append(byte[] data) {
            synchronized (inMemoryChunkDataList) {
                inMemoryChunkDataList.add(InMemoryChunkData.builder()
                        .data(data)
                        .length(data.length)
                        .start(length)
                        .build());
                length += data.length;
            }
        }

        public boolean truncate(long offset) {
            InMemoryChunkData last = null;
            synchronized (inMemoryChunkDataList) {
                for (int i = 0; i < inMemoryChunkDataList.size(); i++) {
                    last = inMemoryChunkDataList.get(0);
                    if (last.start <= offset && last.start + last.length < offset) {
                        break;
                    }
                }
                if (null != last) {
                    int newLength = Math.toIntExact(offset - last.start);
                    if (newLength < last.length) {
                        byte[] newArray = new byte[Math.toIntExact(offset - last.start)];
                        System.arraycopy(last.data, 0, newArray, 0, newLength);
                        last.data = newArray;
                        last.length = newLength;
                    }
                    this.length = offset;
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Stores the actual chunk data.
     */
    @Builder
    static class InMemoryChunkData {
        @Getter
        @Setter
        long start;

        @Getter
        @Setter
        long length;

        @Getter
        @Setter
        byte[] data;
    }
}
