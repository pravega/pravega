/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Storage adapter for file system based storage.
 *
 * Each segment is represented as a single file on the underlying storage.
 *
 * Approach to fencing:
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an offset assigned to it and Pravega always writes the same data to the same offset.
 *
 * With this assumption the only flow when a write call is made to the same offset twice is when ownership of the
 * segment changes from one host to another and both the hosts are writing to it.
 *
 * As write to same offset to a file is idempotent (any attempt to re-write data with the same file offset does not
 * cause any form of inconsistency), locking is not required.
 *
 * In the absence of locking this is the expected behavior in case of ownership change: both the hosts will keep
 * writing the same data at the same offset till the time the earlier owner gets a notification that it is not the
 * current owner. Once the earlier owner received this notification, it stops writing to the segment.
 */
@Slf4j
public class FileSystemStorage implements Storage {
    private static final int NUM_RETRIES = 3;

    //region members

    private final FileSystemStorageConfig config;
    private final ExecutorService executor;
    private final AtomicBoolean closed;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemStorage class.
     *
     * @param config   The configuration to use.
     * @param executor The executor to use for running async operations.
     */
    public FileSystemStorage(FileSystemStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean(false);
        this.config = config;
        this.executor = executor;
    }

    //endregion

    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     *
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(streamSegmentName, () -> syncOpenRead(streamSegmentName));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle,
                                           long offset,
                                           byte[] buffer,
                                           int bufferOffset,
                                           int length,
                                           Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncRead(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncGetStreamSegmentInfo(streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncExists(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return supplyAsync(streamSegmentName, () -> syncOpenWrite(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncCreate(streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle,
                                         long offset,
                                         InputStream data,
                                         int length,
                                         Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncWrite(handle, offset, data, length));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncSeal(handle));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment,
                                          Duration timeout) {
        return supplyAsync(targetHandle.getSegmentName(), () -> syncConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncDelete(handle));
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    //region private sync implementation

    private SegmentHandle syncOpenRead(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
        Path path = Paths.get(config.getRoot(), streamSegmentName);

        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
        return FileSystemSegmentHandle.readHandle(streamSegmentName);
    }

    private SegmentHandle syncOpenWrite(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        Path path = Paths.get(config.getRoot(), streamSegmentName);
        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        } else if (Files.isWritable(path)) {
            LoggerHelpers.traceLeave(log, "openRead", traceId);
            return FileSystemSegmentHandle.writeHandle(streamSegmentName);
        } else {
            LoggerHelpers.traceLeave(log, "openWrite", traceId);
            return syncOpenRead(streamSegmentName);
        }
    }

    private int syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName(), offset, bufferOffset, length);

        Path path = Paths.get(config.getRoot(), handle.getSegmentName());

        long fileSize = Files.size(path);
        if (fileSize < offset) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of segment (%d).", offset, fileSize));
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            int bytesRead = 0;

            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                bytesRead = channel.read(readBuffer, offset);
                bufferOffset += bytesRead;
                length -= bytesRead;
            } while (length != 0);
            LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
            return bytesRead;
        }
    }

    private SegmentProperties syncGetStreamSegmentInfo(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getRoot(), streamSegmentName),
                PosixFileAttributes.class);
        StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(),
                !(attrs.permissions().contains(OWNER_WRITE)), false,
                new ImmutableDate(attrs.creationTime().toMillis()));

        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return information;
    }

    private boolean syncExists(String streamSegmentName) {
        return Files.exists(Paths.get(config.getRoot(), streamSegmentName));
    }

    private SegmentProperties syncCreate(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        Set<PosixFilePermission> perms = new HashSet<>();
        // add permission as rw-r--r-- 644
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.OTHERS_READ);
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(perms);

        Path path = Paths.get(config.getRoot(), streamSegmentName);
        Files.createDirectories(path.getParent());
        Files.createFile(path, fileAttributes);
        LoggerHelpers.traceLeave(log, "create", traceId);
        return this.syncGetStreamSegmentInfo(streamSegmentName);
    }

    private Void syncWrite(SegmentHandle handle, long offset, InputStream data, int length)
            throws IOException, StreamSegmentSealedException, BadOffsetException {
        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException("Write called on a readonly handle of segment "
                    + handle.getSegmentName());
        }

        Path path = Paths.get(config.getRoot(), handle.getSegmentName());

        // Fix for the case where Pravega runs with super user privileges.
        // This means that writes to readonly files also succeed. We need to explicitly check permissions in this case.
        if (!isWritableFile(path)) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        long fileSize = path.toFile().length();
        if (fileSize < offset) {
            throw new BadOffsetException(handle.getSegmentName(), fileSize, offset);
        } else {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
                 ReadableByteChannel sourceChannel = Channels.newChannel(data)) {
                while (length != 0) {
                    long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                    offset += bytesWritten;
                    length -= bytesWritten;
                }
            }
            LoggerHelpers.traceLeave(log, "write", traceId);
            return null;
        }
    }

    private boolean isWritableFile(Path path) throws IOException {
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class);
        return attrs.permissions().contains(OWNER_WRITE);
    }

    private Void syncSeal(SegmentHandle handle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());
        if (handle.isReadOnly()) {
            throw new IllegalArgumentException(handle.getSegmentName());
        }

        Set<PosixFilePermission> perms = new HashSet<>();
        // add permission as r--r--r-- 444
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.OTHERS_READ);
        Files.setPosixFilePermissions(Paths.get(config.getRoot(), handle.getSegmentName()), perms);
        LoggerHelpers.traceLeave(log, "seal", traceId);
        return null;
    }

    /**
     * Concatenation as currently implemented here requires that we read the data and write it back to target file.
     * We do not make the assumption that a native operation exists as this is not a common feature supported by file
     * systems. As such, a concatenation induces an important network overhead as each byte concatenated must be
     * read and written back when the storage is backed by a remote filesystem (through NFS).
     *
     * This option was preferred as other option (of having one file per transaction) will result in server side
     * fragmentation and corresponding slowdown in cluster performance.
     */
    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle.getSegmentName(),
                offset, sourceSegment);

        Path sourcePath = Paths.get(config.getRoot(), sourceSegment);
        Path targetPath = Paths.get(config.getRoot(), targetHandle.getSegmentName());

        long length = Files.size(sourcePath);
        try (FileChannel targetChannel = FileChannel.open(targetPath, StandardOpenOption.WRITE);
             RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
            if (isWritableFile(sourcePath)) {
                throw new IllegalStateException(String.format("Source segment (%s) is not sealed.", sourceSegment));
            }
            while (length > 0) {
                long bytesTransferred = targetChannel.transferFrom(sourceFile.getChannel(), offset, length);
                offset += bytesTransferred;
                length -= bytesTransferred;
            }
            Files.delete(sourcePath);
            LoggerHelpers.traceLeave(log, "concat", traceId);
            return null;
        }
    }

    private Void syncDelete(SegmentHandle handle) throws IOException {
        Files.delete(Paths.get(config.getRoot(), handle.getSegmentName()));
        return null;
    }

    /**
     * Executes the given supplier asynchronously and returns a Future that will be completed with the result.
     */
    private <R> CompletableFuture<R> supplyAsync(String segmentName, Callable<R> operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                handleException(e, segmentName, result);
            }
        });

        return result;
    }

    private <R> void handleException(Throwable e, String segmentName, CompletableFuture<R> result) {
        result.completeExceptionally(translateException(segmentName, e));
    }

    private Throwable translateException(String segmentName, Throwable e) {
        Throwable retVal = e;
        if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
            retVal = new StreamSegmentNotExistsException(segmentName);
        }

        if (e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(segmentName);
        }

        if (e instanceof IndexOutOfBoundsException) {
            retVal = new IllegalArgumentException(e.getMessage());
        }

        if (e instanceof AccessDeniedException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    //endregion
}
