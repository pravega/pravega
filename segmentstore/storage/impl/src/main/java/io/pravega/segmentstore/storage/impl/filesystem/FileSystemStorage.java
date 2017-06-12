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
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Storage adapter for file system based Tier 2.
 *
 * Each segment is represented as a single file on the underlying storage.
 *
 * Approach to locking:
 *
 * This implementation works on the assumption is data written to Tier 2 is always written in append only fashion.
 * Once a piece of data is written it is never overwritten. With this assumption the only flow when a write call is
 * made to the same offset twice is when ownership of the segment changes from one host to another and both the hosts
 * are writing to it.
 * As write to an offset to a file is idempotent (any attempt to re-write data with the same file offset does not
 * cause any form of inconsistency), locking is not required.
 *
 * In the absence of locking this is the expected behavior in case of ownership change: both the hosts will keep
 * writing the same data at the same offset till the time the earlier owner gets a notification that it is not the
 * current owner. Once the earlier owner received this notification from Tier 1, it stops writing to the segment.
 *
 */

@Slf4j
public class FileSystemStorage implements Storage {

    //region members

    private final FileSystemStorageConfig config;
    private final ExecutorService executor;

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
        this.config = config;
        this.executor = executor;
    }

    //endregion

    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {

    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync( streamSegmentName, () -> syncOpenRead(streamSegmentName));
    }


    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        return supplyAsync( handle.getSegmentName(), () -> syncRead(handle, offset, buffer, bufferOffset, length));
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
        return supplyAsync( streamSegmentName, () -> syncCreate(streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration
            timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncWrite(handle, offset, data, length));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncSeal(handle));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration
            timeout) {
            return supplyAsync(targetHandle.getSegmentName(),
                    () -> syncConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
            return supplyAsync(handle.getSegmentName(), () -> syncDelete(handle));
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {

    }

    //endregion

    //region private sync implementation

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private SegmentHandle syncOpenRead(String streamSegmentName) {
        Path path = Paths.get(config.getFilesystemRoot(), streamSegmentName);

        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        return FileSystemSegmentHandle.getReadHandle(streamSegmentName);
    }

    @SneakyThrows
    private SegmentHandle syncOpenWrite(String streamSegmentName) {
        Path path = Paths.get(config.getFilesystemRoot(), streamSegmentName);
        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        } else if (Files.isWritable(path)) {
            return FileSystemSegmentHandle.getWriteHandle(streamSegmentName);
        } else {
                return openRead(streamSegmentName).get();
        }
    }

    @SneakyThrows(IOException.class)
    private int syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) {

        Path path = Paths.get(config.getFilesystemRoot(), handle.getSegmentName());

            if (Files.size(path) < offset) {
                log.warn("Read called on segment {} at offset {}. The offset is beyond the current size of the file.",
                        handle.getSegmentName(), offset);
                throw new IllegalArgumentException( "Reading beyond the current size of segment");
            }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            int bytesRead = channel.read(ByteBuffer.wrap(buffer, bufferOffset, length), offset);
            return bytesRead;
        }
    }

    @SneakyThrows(IOException.class)
    private SegmentProperties syncGetStreamSegmentInfo(String streamSegmentName) {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getFilesystemRoot(), streamSegmentName),
                    PosixFileAttributes.class);
            StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(),
                    !(attrs.permissions().contains(OWNER_WRITE)), false,
                    new ImmutableDate(attrs.creationTime().toMillis()));

            return information;
    }

    private boolean syncExists(String streamSegmentName) {
        boolean exists = Files.exists(Paths.get(config.getFilesystemRoot(), streamSegmentName));
        return exists;
    }

    @SneakyThrows
    private SegmentProperties syncCreate(String streamSegmentName) {
        log.info("Creating Segment {}", streamSegmentName);
            Set<PosixFilePermission> perms = new HashSet<>();
            // add permission as rw-r--r-- 644
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(perms);

            Path path = Paths.get(config.getFilesystemRoot(), streamSegmentName);
            Files.createDirectories(path.getParent());
            Files.createFile(path, fileAttributes);
            log.info("Created Segment {}", streamSegmentName);
            return this.getStreamSegmentInfo(streamSegmentName, null).get();
    }

    @SneakyThrows
    private Void syncWrite(SegmentHandle handle, long offset, InputStream data, int length) {
        log.trace("Writing {} to segment {} at offset {}", length, handle.getSegmentName(), offset);
        Path path = Paths.get(config.getFilesystemRoot(), handle.getSegmentName());

        if (handle.isReadOnly()) {
            log.warn("Write called on a readonly handle of segment {}", handle.getSegmentName());
            throw new IllegalArgumentException("Write called on a readonly handle of segment "
                    + handle.getSegmentName());
        }

        //Fix for Jenkins as Jenkins runs as super user privileges.
        if ( !isWritableFile(path)) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        long fileSize = path.toFile().length();
        if (fileSize < offset) {
            throw new BadOffsetException(handle.getSegmentName(), fileSize, offset);
        } else {
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.WRITE); ReadableByteChannel sourceChannel = Channels.newChannel(data)) {
                while (length != 0) {
                    long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                    offset += bytesWritten;
                    length -= bytesWritten;
                }
            }
            return null;
        }
    }

    private boolean isWritableFile(Path path) throws IOException {
        PosixFileAttributes attrs = null;
            attrs = Files.readAttributes(path,
                    PosixFileAttributes.class);
            return attrs.permissions().contains(OWNER_WRITE);
    }

    @SneakyThrows
    private Void syncSeal(SegmentHandle handle) {

        if (handle.isReadOnly()) {
            log.info("Seal called on a read handle for segment {}", handle.getSegmentName());
            throw new IllegalArgumentException(handle.getSegmentName());
        }
            Set<PosixFilePermission> perms = new HashSet<>();
            // add permission as r--r--r-- 444
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(Paths.get(config.getFilesystemRoot(), handle.getSegmentName()), perms);
            log.info("Successfully sealed segment {}", handle.getSegmentName());
            return null;
    }

    @SneakyThrows(IOException.class)
    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment) {

        Path sourcePath = Paths.get(config.getFilesystemRoot(), sourceSegment);
        Path targetPath = Paths.get(config.getFilesystemRoot(), targetHandle.getSegmentName());

        try (FileChannel targetChannel = (FileChannel) Files.newByteChannel(targetPath, EnumSet.of(StandardOpenOption.APPEND));
             RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
            if (isWritableFile(sourcePath)) {
                throw new IllegalStateException(sourceSegment);
            }

            targetChannel.transferFrom(sourceFile.getChannel(), offset, sourceFile.length());
            Files.delete(Paths.get(config.getFilesystemRoot(), sourceSegment));
            return null;
        }
    }

    @SneakyThrows(IOException.class)
    private Void syncDelete(SegmentHandle handle) {
            Files.delete(Paths.get(config.getFilesystemRoot(), handle.getSegmentName()));
            return null;
    }

    /**
     * Executes the given supplier asynchronously and returns a Future that will be completed with the result.
     */
    private <R> CompletableFuture<R> supplyAsync(String segmentName, Supplier<R> operation) {
        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.get());
            } catch (Exception e) {
                handleException(e, segmentName, result);
            }
        });

        return result;
    }

    private <R> void handleException(Exception e, String segmentName, CompletableFuture<R> result) {
        result.completeExceptionally( translateException(segmentName, e));
    }

    private Exception translateException(String segmentName, Exception e) {
        Exception retVal = e;
        if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
                retVal = new StreamSegmentNotExistsException(segmentName);
        }

        if ( e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(segmentName);
        }

        if ( e instanceof IndexOutOfBoundsException) {
            retVal = new IllegalArgumentException(e.getMessage());
        }

        if (e instanceof AccessDeniedException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    //endregion

}
