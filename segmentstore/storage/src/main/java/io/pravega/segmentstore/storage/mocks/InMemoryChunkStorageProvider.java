/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
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

/**
 * In-Memory mock for ChunkStorageProvider. Contents is destroyed when object is garbage collected.
 */
@Slf4j
public class InMemoryChunkStorageProvider extends AbstractInMemoryChunkStorageProvider {
    private final ConcurrentHashMap<String, InMemoryChunk> chunks = new ConcurrentHashMap<String, InMemoryChunk>();

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        InMemoryChunk chunk = chunks.get(chunkName);
        if (null == chunk) {
            throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorageProvider::doGetInfo");
        }
        return ChunkInfo.builder()
                .length(chunk.getLength())
                .name(chunkName)
                .build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        if (null != chunks.putIfAbsent(chunkName, new InMemoryChunk(chunkName))) {
            throw new ChunkAlreadyExistsException(chunkName, "InMemoryChunkStorageProvider::doCreate");
        }
        return new ChunkHandle(chunkName, false);
    }

    @Override
    protected boolean doesExist(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        return chunks.containsKey(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        if (null == chunk) {
            throw new ChunkNotFoundException(handle.getChunkName(), "InMemoryChunkStorageProvider::doDelete");
        }
        if (chunk.isReadOnly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
        }
        chunks.remove(handle.getChunkName());
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        if (chunks.containsKey(chunkName)) {
            return new ChunkHandle(chunkName, true);
        }
        throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorageProvider::doOpenRead");
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        if (chunks.containsKey(chunkName)) {
            return new ChunkHandle(chunkName, false);
        }
        throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorageProvider::doOpenWrite");
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        if (fromOffset >= chunk.getLength()) {
            throw new ChunkStorageException(handle.getChunkName(), "Attempt to read at wrong offset");
        }
        if (length == 0) {
            throw new ChunkStorageException(handle.getChunkName(), "Attempt to read 0 bytes");
        }

        // This is implemented this way to simulate possibility of partial read.
        InMemoryChunkData matchingData = null;

        // TODO : This is OK for now. This is just test code, but need binary search here.
        for (InMemoryChunkData data : chunk.inMemoryChunkDataList) {
            if (data.start <= fromOffset && data.start + data.length > fromOffset) {
                matchingData = data;
                break;

            }
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
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IndexOutOfBoundsException, ChunkStorageException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        long oldLength = chunk.getLength();
        if (offset != chunk.getLength()) {
            throw new IndexOutOfBoundsException("Attempt to write at wrong offset");
        }
        if (length == 0) {
            throw new IndexOutOfBoundsException("Attempt to write 0 bytes");
        }
        if (chunk.isReadOnly) {
            throw new ChunkStorageException(handle.getChunkName(), "chunk is readonly");
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
        Preconditions.checkState(length == totalBytesRead);

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
            throw new ChunkNotFoundException(chunkName, "InMemoryChunkStorageProvider::getInMemoryChunk");
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
                delete(ChunkHandle.writeHandle(source.getName()));
                total += chunks[i].getLength();
            }
            return total;
        }
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        return chunk.truncate(offset);
    }

    @Override
    protected boolean doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        chunk.setReadOnly(isReadOnly);
        return true;
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