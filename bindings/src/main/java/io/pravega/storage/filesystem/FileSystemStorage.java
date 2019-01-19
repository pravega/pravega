/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
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
import java.security.AccessControlException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
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
public class FileSystemStorage implements SyncStorage {
    //region members

    private static final Set<PosixFilePermission> READ_ONLY_PERMISSION = ImmutableSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ);

    private static final Set<PosixFilePermission> READ_WRITE_PERMISSION = ImmutableSet.of(
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ);

    private final FileSystemStorageConfig config;
    private final AtomicBoolean closed;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemStorage class.
     *
     * @param config   The configuration to use.
     */
    public FileSystemStorage(FileSystemStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.closed = new AtomicBoolean(false);

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
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenRead(streamSegmentName));
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        return execute(handle.getSegmentName(), () -> doRead(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doGetStreamSegmentInfo(streamSegmentName));
    }

    @Override
    @SneakyThrows(StreamSegmentException.class)
    public boolean exists(String streamSegmentName) {
        return execute(streamSegmentName, () -> doExists(streamSegmentName));
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenWrite(streamSegmentName));
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doCreate(streamSegmentName));
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doWrite(handle, offset, data, length));
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doSeal(handle));
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doUnseal(handle));
    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
        execute(targetHandle.getSegmentName(), () -> doConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doDelete(handle));
    }


    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    //region private sync implementation

    private SegmentHandle doOpenRead(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
        Path path = Paths.get(config.getRoot(), streamSegmentName);

        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
        return FileSystemSegmentHandle.readHandle(streamSegmentName);
    }

    private SegmentHandle doOpenWrite(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        Path path = Paths.get(config.getRoot(), streamSegmentName);
        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        } else if (Files.isWritable(path)) {
            LoggerHelpers.traceLeave(log, "openWrite", traceId);
            return FileSystemSegmentHandle.writeHandle(streamSegmentName);
        } else {
            LoggerHelpers.traceLeave(log, "openWrite", traceId);
            return FileSystemSegmentHandle.readHandle(streamSegmentName);
        }
    }

    private int doRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName(), offset, bufferOffset, length);
        Timer timer = new Timer();

        Path path = Paths.get(config.getRoot(), handle.getSegmentName());

        long fileSize = Files.size(path);
        if (fileSize < offset) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of segment (%d).", offset, fileSize));
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            int totalBytesRead = 0;

            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                int bytesRead = channel.read(readBuffer, offset);
                bufferOffset += bytesRead;
                totalBytesRead += bytesRead;
                length -= bytesRead;
            } while (length != 0);
            FileSystemMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
            FileSystemMetrics.READ_BYTES.add(totalBytesRead);
            LoggerHelpers.traceLeave(log, "read", traceId, totalBytesRead);
            return totalBytesRead;
        }
    }

    private SegmentProperties doGetStreamSegmentInfo(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getRoot(), streamSegmentName),
                PosixFileAttributes.class);
        StreamSegmentInformation information = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(attrs.size())
                .sealed(!(attrs.permissions().contains(OWNER_WRITE)))
                .lastModified(new ImmutableDate(attrs.creationTime().toMillis()))
                .build();

        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return information;
    }

    private boolean doExists(String streamSegmentName) {
        return Files.exists(Paths.get(config.getRoot(), streamSegmentName));
    }

    private SegmentHandle doCreate(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(READ_WRITE_PERMISSION);

        Path path = Paths.get(config.getRoot(), streamSegmentName);
        Path parent = path.getParent();
        assert parent != null;
        Files.createDirectories(parent);
        Files.createFile(path, fileAttributes);
        LoggerHelpers.traceLeave(log, "create", traceId);
        FileSystemMetrics.CREATE_COUNT.inc();
        return FileSystemSegmentHandle.writeHandle(streamSegmentName);
    }

    private Void doWrite(SegmentHandle handle, long offset, InputStream data, int length) throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);
        Timer timer = new Timer();

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException("Write called on a readonly handle of segment " + handle.getSegmentName());
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
            long totalBytesWritten = 0;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                // Wrap the input data into a ReadableByteChannel, but do not close it. Doing so will result in closing
                // the underlying InputStream, which is not desirable if it is to be reused.
                ReadableByteChannel sourceChannel = Channels.newChannel(data);
                while (length != 0) {
                    long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                    assert bytesWritten > 0 : "Unable to make any progress transferring data.";
                    offset += bytesWritten;
                    totalBytesWritten += bytesWritten;
                    length -= bytesWritten;
                }
            }
            FileSystemMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
            FileSystemMetrics.WRITE_BYTES.add(totalBytesWritten);
            LoggerHelpers.traceLeave(log, "write", traceId);
            return null;
        }
    }

    private boolean isWritableFile(Path path) throws IOException {
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class);
        return attrs.permissions().contains(OWNER_WRITE);
    }

    private Void doSeal(SegmentHandle handle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());
        if (handle.isReadOnly()) {
            throw new IllegalArgumentException(handle.getSegmentName());
        }

        Files.setPosixFilePermissions(Paths.get(config.getRoot(), handle.getSegmentName()), READ_ONLY_PERMISSION);
        LoggerHelpers.traceLeave(log, "seal", traceId);
        return null;
    }

    private Void doUnseal(SegmentHandle handle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "unseal", handle.getSegmentName());
        Files.setPosixFilePermissions(Paths.get(config.getRoot(), handle.getSegmentName()), READ_WRITE_PERMISSION);
        LoggerHelpers.traceLeave(log, "unseal", traceId);
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
    private Void doConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws IOException {
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

    private Void doDelete(SegmentHandle handle) throws IOException {
        Files.delete(Paths.get(config.getRoot(), handle.getSegmentName()));
        return null;
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param segmentName   Full name of the StreamSegment.
     * @param operation     The function to execute.
     * @param <R>           Return type of the operation.
     * @return Instance of the return type of the operation.
     */
    private <R> R execute(String segmentName, Callable<R> operation) throws StreamSegmentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            return operation.call();
        } catch (Exception e) {
            return throwException(segmentName, e);
        }
    }

    private <T> T throwException(String segmentName, Exception e) throws StreamSegmentException {
        if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
            throw new StreamSegmentNotExistsException(segmentName);
        }

        if (e instanceof FileAlreadyExistsException) {
            throw new StreamSegmentExistsException(segmentName);
        }

        if (e instanceof IndexOutOfBoundsException) {
            throw new IllegalArgumentException(e.getMessage());
        }

        if (e instanceof AccessControlException
                || e instanceof AccessDeniedException
                || e instanceof NonWritableChannelException) {
            throw new StreamSegmentSealedException(segmentName, e);
        }

        throw Exceptions.sneakyThrow(e);
    }

    //endregion
}
