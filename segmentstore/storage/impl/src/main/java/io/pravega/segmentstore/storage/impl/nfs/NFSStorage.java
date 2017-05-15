/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.nfs;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Slf4j
public class NFSStorage implements Storage {
    private final NFSStorageConfig config;
    private final ExecutorService executor;

    public NFSStorage(NFSStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public void initialize(long containerEpoch) {

    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        CompletableFuture<SegmentHandle> retVal = new CompletableFuture<>();
        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(config.getNfsRoot(),
                    streamSegmentName), StandardOpenOption.READ);
            NFSSegmentHandle retHandle = NFSSegmentHandle.getReadHandle(streamSegmentName, channel);
            retVal.complete(retHandle);
        } catch (IOException ex) {
            retVal.completeExceptionally(ex);
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        CompletableFuture<Integer> retVal = new CompletableFuture<>();
        ((NFSSegmentHandle)handle).channel.read(ByteBuffer.wrap(buffer), bufferOffset, null, new CompletionHandler
                <Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                retVal.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                retVal.completeExceptionally(exc);
            }
        });
        return retVal;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getNfsRoot(), streamSegmentName),
                    PosixFileAttributes.class);
            StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(),
                    ! (attrs.permissions().contains(PosixFilePermission.OWNER_WRITE)), false, new ImmutableDate
                    (attrs.creationTime().toMillis()));
            retVal.complete(information);
        } catch (IOException e) {
            retVal.completeExceptionally(e);
        }

        return retVal;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return null;
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return null;
    }

    @Override
    public void close() {

    }
}
