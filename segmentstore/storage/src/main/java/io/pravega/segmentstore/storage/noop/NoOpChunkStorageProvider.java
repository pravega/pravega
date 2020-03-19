/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorageProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/*
 NoOp implementation.
 */
public class NoOpChunkStorageProvider extends AbstractInMemoryChunkStorageProvider {
    @Getter
    @Setter
    ConcurrentHashMap<String, ChunkData> chunkMetadata = new ConcurrentHashMap<>();


    public NoOpChunkStorageProvider(Executor executor) {
        super(executor);
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new FileNotFoundException(chunkName);
        }
        return ChunkInfo.builder().name(chunkName).length(chunkData.length).build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null != chunkData) {
            throw new FileAlreadyExistsException(chunkName);
        }
        chunkMetadata.put(chunkName, new ChunkData());
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean doesExist(String chunkName) throws IOException, IllegalArgumentException {
        return chunkMetadata.containsKey(chunkName);
    }

    @Override
    protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        chunkMetadata.remove(handle.getChunkName());
        return true;
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new FileNotFoundException(chunkName);
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException {
        ChunkData chunkData = chunkMetadata.get(chunkName);
        if (null == chunkData) {
            throw new FileNotFoundException(chunkName);
        }
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        // TODO: Add validation
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new FileNotFoundException(handle.getChunkName());
        }

        if (fromOffset >= chunkData.length || fromOffset + length > chunkData.length) {
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
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IOException, IndexOutOfBoundsException {
        // TODO: Add validation
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new FileNotFoundException(handle.getChunkName());
        }
        if (offset != chunkData.length) {
            throw new IndexOutOfBoundsException("");
        }
        chunkData.length = offset + length;
        chunkMetadata.put(handle.getChunkName(), chunkData);
        return length;
    }

    @Override
    protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        int total = 0;
        val targetChunkData = chunkMetadata.get(target.getChunkName());
        for (ChunkHandle chunkHandle : sources) {
            val chunkData = chunkMetadata.get(chunkHandle.getChunkName());
            total += chunkData.length;
            chunkMetadata.remove(chunkHandle.getChunkName());
        }
        targetChunkData.length += total;
        return total;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        return false;
    }

    @Override
    protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException {
        Preconditions.checkNotNull(null != handle, "handle");
        Preconditions.checkNotNull(handle.getChunkName(), "handle");
        String chunkName = handle.getChunkName();
        ChunkData chunkData = chunkMetadata.get(handle.getChunkName());
        if (null == chunkData) {
            throw new FileNotFoundException(chunkName);
        }
        chunkData.isReadonly = isReadOnly;
        return false;
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
    public static class ChunkData {
        public long length;
        public boolean isReadonly;
    }
}
