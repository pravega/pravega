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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
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
import java.util.concurrent.ExecutorService;

import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

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
            if ( ex instanceof NoSuchFileException) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName, ex));
            } else {
                retVal.completeExceptionally(ex);
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        CompletableFuture<Integer> retVal = new CompletableFuture<>();
        if (((NFSSegmentHandle) handle).channel == null || !Files.exists(Paths.get(config.getNfsRoot(),
                handle.getSegmentName()))) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        } else {
            try {
                ((NFSSegmentHandle) handle).channel.read(ByteBuffer.wrap(buffer, bufferOffset, length), offset, null,
                        new CompletionHandler<Integer, Object>() {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                if (result == -1) {
                                    retVal.completeExceptionally(new IllegalArgumentException(handle.getSegmentName()));
                                } else {
                                    retVal.complete(result);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, Object attachment) {
                                retVal.completeExceptionally(exc);
                            }
                        });
            } catch (Exception e) {
                if ( e instanceof IndexOutOfBoundsException) {
                    retVal.completeExceptionally(new ArrayIndexOutOfBoundsException(e.getMessage()));
                } else {
                    retVal.completeExceptionally(e);
                }
            }
        }

        return retVal;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getNfsRoot(), streamSegmentName),
                    PosixFileAttributes.class);
            StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(),
                    !(attrs.permissions().contains(OWNER_WRITE)), false,
                    new ImmutableDate(attrs.creationTime().toMillis()));

            retVal.complete(information);
        } catch (IOException e) {
            retVal.completeExceptionally(e);
        }

        return retVal;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        CompletableFuture<Boolean> retFuture = new CompletableFuture<>();
        boolean exists = Files.exists(Paths.get(config.getNfsRoot(), streamSegmentName));
        retFuture.complete(exists);
        return retFuture;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        CompletableFuture<SegmentHandle> retVal = new CompletableFuture<>();
        try {
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(config.getNfsRoot(),
                    streamSegmentName), StandardOpenOption.WRITE, StandardOpenOption.READ);
            NFSSegmentHandle retHandle = NFSSegmentHandle.getWriteHandle(streamSegmentName, channel);
            retVal.complete(retHandle);
        } catch (IOException ex) {
            if ( ex instanceof NoSuchFileException) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName, ex));
            } else if (ex instanceof AccessDeniedException) {
                retVal = openRead(streamSegmentName);
            } else {
                retVal.completeExceptionally(ex);
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {

        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
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
        Files.createFile(path, fileAttributes );
        retVal = this.getStreamSegmentInfo(streamSegmentName, timeout);
        } catch (IOException e) {
            if (e instanceof FileAlreadyExistsException) {
                retVal.completeExceptionally(new StreamSegmentExistsException(streamSegmentName, e));
            } else {
                retVal.completeExceptionally(e);
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if (((NFSSegmentHandle) handle).channel == null ||
                !Files.exists(Paths.get(config.getNfsRoot(), handle.getSegmentName()))) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        } else {
            try {
                if (((NFSSegmentHandle) handle).channel.size() < offset) {
                    retVal.completeExceptionally(new BadOffsetException(handle.getSegmentName(),
                            ((NFSSegmentHandle) handle).channel.size(), offset));
                } else {
                    byte[] bytes = new byte[length];
                    try {
                        data.read(bytes);
                        ((NFSSegmentHandle) handle).channel.write(ByteBuffer.wrap(bytes), offset, null, new CompletionHandler<Integer, Object>() {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                retVal.complete(null);
                            }

                            @Override
                            public void failed(Throwable exc, Object attachment) {
                                if (exc instanceof NonWritableChannelException) {
                                    retVal.completeExceptionally(new IllegalArgumentException(exc));
                                } else if (exc instanceof ClosedChannelException) {
                                    retVal.completeExceptionally(
                                            new StreamSegmentSealedException(handle.getSegmentName(), exc));
                                } else {
                                    retVal.completeExceptionally(exc);
                                }
                            }
                        });
                    } catch (Exception exc) {
                        if (exc instanceof NonWritableChannelException) {
                            retVal.completeExceptionally(new IllegalArgumentException(exc));
                        } else if (exc instanceof ClosedChannelException) {
                            retVal.completeExceptionally(
                                    new StreamSegmentSealedException(handle.getSegmentName(), exc));
                        } else {
                            retVal.completeExceptionally(exc);
                        }
                    }
                }
            } catch (Exception exc) {
                if (exc instanceof NonWritableChannelException) {
                    retVal.completeExceptionally(new IllegalArgumentException(exc));
                } else if (exc instanceof ClosedChannelException) {
                    retVal.completeExceptionally(
                            new StreamSegmentSealedException(handle.getSegmentName(), exc));
                } else {
                    retVal.completeExceptionally(exc);
                }
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        if (((NFSSegmentHandle) handle).channel == null) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        } else if (handle.isReadOnly()) {
            retVal.completeExceptionally(new IllegalArgumentException(handle.getSegmentName()));
        } else {
            try {
                ((NFSSegmentHandle) handle).channel.close();
                Set<PosixFilePermission> perms = new HashSet<>();
                // add permission as r--r--r-- 444
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.GROUP_READ);
                perms.add(PosixFilePermission.OTHERS_READ);
                Files.setPosixFilePermissions(Paths.get(config.getNfsRoot(), handle.getSegmentName()), perms);
                retVal.complete(null);
            } catch (IOException e) {
                if (e instanceof NoSuchFileException) {
                    retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), e));
                } else {
                    retVal.completeExceptionally(e);
                }
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if (((NFSSegmentHandle) targetHandle).channel == null) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(targetHandle.getSegmentName()));
        } else if (Files.isWritable(Paths.get(config.getNfsRoot(), sourceSegment))) {
            retVal.completeExceptionally(new IllegalStateException(sourceSegment));
        } else {
            try {
                ((NFSSegmentHandle) targetHandle).channel.force(true);

                FileChannel channel = new RandomAccessFile(
                        String.valueOf(Paths.get(config.getNfsRoot(), targetHandle.getSegmentName())), "rw")
                        .getChannel();
                RandomAccessFile sourceFile = new RandomAccessFile(
                        String.valueOf(Paths.get(config.getNfsRoot(), sourceSegment)), "r");
                channel.transferFrom(sourceFile.getChannel(), offset, sourceFile.length());
                Files.delete(Paths.get(config.getNfsRoot(), sourceSegment));
                retVal.complete(null);
            } catch (IOException e) {
                if ( e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
                    retVal.completeExceptionally(new StreamSegmentNotExistsException(targetHandle.getSegmentName()));
                } else {
                    retVal.completeExceptionally(e);
                }
            }
        }
        return retVal;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Files.delete(Paths.get(config.getNfsRoot(), handle.getSegmentName()));
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() {

    }
}
