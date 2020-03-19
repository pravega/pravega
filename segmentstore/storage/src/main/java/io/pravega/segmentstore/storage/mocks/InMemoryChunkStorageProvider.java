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
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * In-Memory mock for ChunkStorageProvider. Contents is destroyed when object is garbage collected.
 */
public class InMemoryChunkStorageProvider extends AbstractInMemoryChunkStorageProvider {
    ConcurrentHashMap<String, InMemoryChunk> chunks = new ConcurrentHashMap<String, InMemoryChunk>();

    //region Members
    public InMemoryChunkStorageProvider(Executor executor) {
        super(executor);
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        InMemoryChunk chunk = chunks.get(chunkName);
        if (null == chunk) {
            throw new FileNotFoundException(chunkName);
        }
        return ChunkInfo.builder()
                .length(chunk.getLength())
                .name(chunkName)
                .build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException {
        Preconditions.checkNotNull(chunkName);
        if (null != chunks.putIfAbsent(chunkName, new InMemoryChunk(chunkName))) {
            throw new FileAlreadyExistsException(chunkName);
        }
        return new ChunkHandle(chunkName, false);
    }

    @Override
    protected boolean doesExist(String chunkName) throws IOException, IllegalArgumentException {
        return chunks.containsKey(chunkName);
    }

    @Override
    protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException {
        chunks.remove(handle.getChunkName());
        return true;
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException {
        if (chunks.containsKey(chunkName)) {
            return new ChunkHandle(chunkName, true);
        }
        throw new FileNotFoundException(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException {
        if (chunks.containsKey(chunkName)) {
            return new ChunkHandle(chunkName, false);
        }
        throw new FileNotFoundException(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        if (fromOffset >=  chunk.getLength()) {
            throw new IOException("Attempt to read at wrong offset");
        }
        if (length == 0) {
            throw new IOException("Attempt to read 0 bytes");
        }

        // This is implemented this way to simulate possibility of partial read.
        InMemoryChunkData matchingData = null;

        // TODO : This is OK for now. This is just test code, but need binary search here.
        for (InMemoryChunkData data : chunk.inMemoryChunkDataList) {
            if ( data.start <= fromOffset && data.start + data.length > fromOffset) {
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
        throw new IOException("No data was read");
    }

    @Override
    protected int  doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IndexOutOfBoundsException, IOException {
        InMemoryChunk chunk = getInMemoryChunk(handle);
        long oldLength = chunk.getLength();
        if (offset !=  chunk.getLength()) {
            throw new IndexOutOfBoundsException("Attempt to write at wrong offset");
        }
        if (length == 0) {
            throw new IndexOutOfBoundsException("Attempt to write 0 bytes");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(length);
        byte[] bytes = new byte[length];
        int totalBytesRead = 0;
        int bytesRead = 0;
        while ((bytesRead = data.read(bytes)) != -1) {
            out.write(bytes, totalBytesRead, bytesRead);
            totalBytesRead += bytesRead;
        }
        Preconditions.checkState(length == totalBytesRead);

        byte[] writtenBytes = out.toByteArray();
        Preconditions.checkState(writtenBytes.length == totalBytesRead);
        chunk.append(writtenBytes);
        Preconditions.checkState(oldLength + totalBytesRead == chunk.getLength());
        return totalBytesRead;
    }

    private InMemoryChunk getInMemoryChunk(ChunkHandle handle) throws FileNotFoundException {
        String chunkName = handle.getChunkName();
        InMemoryChunk retValue = chunks.get(chunkName);
        if (null == retValue) {
            throw new FileNotFoundException(handle.getChunkName());
        }
        return retValue;
    }

    @Override
    protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        InMemoryChunk targetChunk = getInMemoryChunk(target);
        synchronized (targetChunk) {
            for (ChunkHandle source : sources) {
                InMemoryChunk chunk = getInMemoryChunk(source);
                for (InMemoryChunkData inMemoryChunkData : chunk.getInMemoryChunkDataList()) {
                    targetChunk.append(inMemoryChunkData.getData());
                }
                delete(source);
            }
            return 0;
        }
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        return false;
    }

    @Override
    protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException {
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
        long length;

        @Getter
        @Setter
        String name;

        @Getter
        @Setter
        boolean isReadOnly;

        @Getter
        List<InMemoryChunkData> inMemoryChunkDataList = new ArrayList<>();

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