/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.fs;

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
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Storage adapter for file system based Tier 2.
 *
 * Each segment is represented as a single file on the underlying storage. As the data in Tier 2 is not modified
 * once written, any attempt to re-write data with the same file offset does not cause any form of inconsistency
 * as the bytes are the same and they are in the same position.
 */

@Slf4j
public class FSStorage implements Storage {

    //region members

    private final FSStorageConfig config;
    private final ExecutorService executor;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FSStorage class.
     *
     * @param config   The configuration to use.
     * @param executor The executor to use for running async operations.
     */
    public FSStorage(FSStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    //endregion

    //region Storage implementation
    @Override
    public void initialize(long containerEpoch) {

    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return CompletableFuture.supplyAsync( () -> syncOpenRead(streamSegmentName),
                executor);
    }


    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> syncRead(handle, offset, buffer, bufferOffset, length, timeout),
                executor);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> syncGetStreamSegmentInfo(streamSegmentName, timeout), executor);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> syncExists(streamSegmentName, timeout), executor);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return CompletableFuture.supplyAsync(() -> {
            Path path = Paths.get(config.getNfsRoot(), streamSegmentName);
            if (!Files.exists(path)) {
                throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
            } else if (Files.isWritable(path)) {
                return FSSegmentHandle.getWriteHandle(streamSegmentName);
            } else {
                try {
                    return openRead(streamSegmentName).get();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }
        }, executor);
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {

        return CompletableFuture.supplyAsync( () -> syncCreate(streamSegmentName, timeout), executor);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration
            timeout) {
        return CompletableFuture.supplyAsync(() -> syncWrite(handle, offset, data, length, timeout), executor);
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> syncSeal(handle, timeout), executor);
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration
            timeout) {
            return CompletableFuture.supplyAsync(() -> syncConcat(targetHandle, offset, sourceSegment, timeout),
                    executor);
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> syncDelete(handle, timeout), executor);
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {

    }

    //endregion

    //region private sync implementation

    private SegmentHandle syncOpenRead(String streamSegmentName) {
        Path path = Paths.get(config.getNfsRoot(), streamSegmentName);

        if (!Files.exists(path)) {
            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
        }

        FSSegmentHandle retHandle = FSSegmentHandle.getReadHandle(streamSegmentName);
        return retHandle;
    }


    private int syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration
            timeout) {

        Path path = Paths.get(config.getNfsRoot(), handle.getSegmentName());

        if (!Files.exists(path)) {
            throw new CompletionException(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        }

        try {
            if (Files.size(path) < offset) {
                log.info("Read called on segment {} at offset {}. The offset is beyond the current size of the file.",
                        handle.getSegmentName(), offset);
                throw new CompletionException(new ArrayIndexOutOfBoundsException());
            }
        } catch (IOException e) {
            throw new CompletionException(e);
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            int bytesRead = channel.read(ByteBuffer.wrap(buffer, bufferOffset, length), offset);
            return bytesRead;
        } catch (Exception e) {
            if (e instanceof IndexOutOfBoundsException) {
                throw new CompletionException(new ArrayIndexOutOfBoundsException(e.getMessage()));
            } else {
                throw new CompletionException(e);
            }
        }
    }




    private SegmentProperties syncGetStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getNfsRoot(), streamSegmentName),
                    PosixFileAttributes.class);
            StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(),
                    !(attrs.permissions().contains(OWNER_WRITE)), false,
                    new ImmutableDate(attrs.creationTime().toMillis()));

            return information;
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    private boolean syncExists(String streamSegmentName, Duration timeout) {
        boolean exists = Files.exists(Paths.get(config.getNfsRoot(), streamSegmentName));
        return exists;
    }


    private SegmentProperties syncCreate(String streamSegmentName, Duration timeout) {
        log.info("Creating Segment {}", streamSegmentName);
        try {
            Set<PosixFilePermission> perms = new HashSet<>();
            // add permission as rw-r--r-- 644
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(perms);

            Path path = Paths.get(config.getNfsRoot(), streamSegmentName);
            Files.createDirectories(path.getParent());
            Files.createFile(path, fileAttributes);
            log.info("Created Segment {}", streamSegmentName);
            return this.getStreamSegmentInfo(streamSegmentName, timeout).get();
        } catch (Exception e) {
            log.info("Exception {} while creating a segment {}", e, streamSegmentName);
            if (e instanceof FileAlreadyExistsException) {
                throw new CompletionException(new StreamSegmentExistsException(streamSegmentName, e));
            } else {
                throw new CompletionException(e);
            }
        }
    }

    private Void syncWrite(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        log.trace("Writing {} to segment {} at offset {}", length, handle.getSegmentName(), offset);
        Path path = Paths.get(config.getNfsRoot(), handle.getSegmentName());

        if (handle.isReadOnly()) {
            log.info("Write called on a readonly handle of segment {}", handle.getSegmentName());
            throw new CompletionException(new IllegalArgumentException());
        }

        if (!Files.exists(path)) {
            throw new CompletionException(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        }

        try {
            if (!isWritableFile(path)) {
                throw new CompletionException(new StreamSegmentSealedException(handle.getSegmentName()));
            }

            long fileSize = path.toFile().length();
            if (fileSize < offset) {
                throw new CompletionException(new BadOffsetException(handle.getSegmentName(), fileSize, offset));
            } else {
                try (FileChannel channel = FileChannel.open(path,
                        StandardOpenOption.WRITE); ReadableByteChannel sourceChannel = Channels.newChannel(data)) {
                    long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                    channel.force(true);
                }
                return null;
            }
        } catch (IOException exc) {
            log.info("Write to segment {} at offset {} failed with exception {} ", handle.getSegmentName(), offset,
                    exc.getMessage());
            if (exc instanceof AccessDeniedException) {
                throw new CompletionException(new IllegalStateException(handle.getSegmentName()));
            } else if (exc instanceof ClosedChannelException) {
                throw new CompletionException(new StreamSegmentSealedException(handle.getSegmentName(), exc));
            } else {
                throw new CompletionException(exc);
            }
        }

    }

    private boolean isWritableFile(Path path) throws IOException {
        PosixFileAttributes attrs = null;
            attrs = Files.readAttributes(path,
                    PosixFileAttributes.class);
            return attrs.permissions().contains(OWNER_WRITE);

    }


    private Void syncSeal(SegmentHandle handle, Duration timeout) {

        if (handle.isReadOnly()) {
            log.info("Seal called on a read handle for segment {}", handle.getSegmentName());
            throw new CompletionException(new IllegalArgumentException(handle.getSegmentName()));
        }

        try {
            Set<PosixFilePermission> perms = new HashSet<>();
            // add permission as r--r--r-- 444
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(Paths.get(config.getNfsRoot(), handle.getSegmentName()), perms);
            log.info("Successfully sealed segment {}", handle.getSegmentName());
            return null;
        } catch (IOException e) {
            log.info("Seal failed with {} for segment {}", e, handle.getSegmentName());
            if (e instanceof NoSuchFileException) {
                throw new CompletionException(new StreamSegmentNotExistsException(handle.getSegmentName(), e));
            } else {
                throw new CompletionException(e);
            }
        }
    }


    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {

        Path sourcePath = Paths.get(config.getNfsRoot(), sourceSegment);
        Path targetPath = Paths.get(config.getNfsRoot(), targetHandle.getSegmentName());

        try (FileChannel targetChannel = new RandomAccessFile(String.valueOf(targetPath), "rw").getChannel();
             RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
            if (isWritableFile(sourcePath)) {
                throw new CompletionException(new IllegalStateException(sourceSegment));
            }

            targetChannel.transferFrom(sourceFile.getChannel(), offset, sourceFile.length());
            Files.delete(Paths.get(config.getNfsRoot(), sourceSegment));
            return null;
        } catch (IOException e) {
            log.info("Concat of {} on {} failed with {}", sourceSegment, targetHandle.getSegmentName(), e);
            if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
                throw new CompletionException(new StreamSegmentNotExistsException(targetHandle.getSegmentName()));
            } else {
                throw new CompletionException(e);
            }
        }
    }


    private Void syncDelete(SegmentHandle handle, Duration timeout) {
        try {
            Files.delete(Paths.get(config.getNfsRoot(), handle.getSegmentName()));
            return null;
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    //endregion

}
