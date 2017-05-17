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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    //region members

    private final NFSStorageConfig config;
    private final ExecutorService executor;

    //endregion

    //region constructor

    public NFSStorage(NFSStorageConfig config, ExecutorService executor) {
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
        final CompletableFuture<SegmentHandle> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncOpenRead(streamSegmentName, retVal);
        });

        return retVal;
    }


    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        final CompletableFuture<Integer> retVal = new CompletableFuture<>();

        executor.execute( () -> {
         syncRead(handle, offset, buffer, bufferOffset, length, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        final CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();

        executor.execute( () -> {
           syncGetStreamSegmentInfo(streamSegmentName, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        final CompletableFuture<Boolean> retFuture = new CompletableFuture<>();

        executor.execute( () -> {
           syncExists(streamSegmentName, timeout, retFuture);
        });

        return retFuture;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        final CompletableFuture<SegmentHandle>[] retVal = new CompletableFuture[1];
        retVal[0] = new CompletableFuture<>();

        executor.execute(() -> {
            try {
                Path path = Paths.get(config.getNfsRoot(), streamSegmentName);
                if (!Files.exists(path)) {
                    retVal[0].completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName));
                } else {
                    FileChannel channel = new RandomAccessFile(path.toString(), "rw").getChannel();
                    NFSSegmentHandle retHandle = NFSSegmentHandle.getWriteHandle(streamSegmentName, channel);
                    retVal[0].complete(retHandle);
                }
            } catch (IOException ex) {
                if (ex instanceof NoSuchFileException) {
                    retVal[0].completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName, ex));
                } else if (ex instanceof FileNotFoundException) {
                    //This means that file exists but not accessible with the access mode.
                    try {
                        retVal[0].complete(openRead(streamSegmentName).get());
                    } catch (Exception e) {
                        retVal[0].completeExceptionally(e);
                    }
                } else {
                    retVal[0].completeExceptionally(ex);
                }
            }
        });

        return retVal[0];
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {

        final CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();

        executor.execute( () -> {
           syncCreate(streamSegmentName, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
       final CompletableFuture<Void> retVal = new CompletableFuture<>();

       executor.execute( () -> {
          syncWrite(handle, offset, data, length, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncSeal( handle, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncConcat(targetHandle, offset, sourceSegment, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        executor.execute( () -> {
            syncDelete( handle, timeout, future);
        });

        return future;
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {

    }

    //endregion

    //region private sync implementation

    private void syncOpenRead(String streamSegmentName, CompletableFuture<SegmentHandle> retVal) {
        try {
            Path path = Paths.get(config.getNfsRoot(), streamSegmentName);
            if (!Files.exists(path)) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName));
            } else {
                FileChannel channel = new RandomAccessFile(path.toString(), "r").getChannel();
                NFSSegmentHandle retHandle = NFSSegmentHandle.getReadHandle(streamSegmentName, channel);
                retVal.complete(retHandle);
            }
        } catch (Exception ex) {
            if (ex instanceof FileNotFoundException) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName, ex));
            } else {
                retVal.completeExceptionally(ex);
            }
        }
    }


    private void syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration
            timeout, CompletableFuture<Integer> retVal) {
        try {
            Path path = Paths.get(config.getNfsRoot(), handle.getSegmentName());
            if (((NFSSegmentHandle) handle).channel == null || !Files.exists(path)) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
            } else if (Files.size(path) < offset) {
                retVal.completeExceptionally(new ArrayIndexOutOfBoundsException());
            } else {
                int bytesRead = ((NFSSegmentHandle) handle).channel.read(
                        ByteBuffer.wrap(buffer, bufferOffset, length), offset);
                retVal.complete(bytesRead);
            }
        } catch (Exception e) {
            if (e instanceof IndexOutOfBoundsException) {
                retVal.completeExceptionally(new ArrayIndexOutOfBoundsException(e.getMessage()));
            } else {
                retVal.completeExceptionally(e);
            }
        }
    }


    private void syncGetStreamSegmentInfo(String streamSegmentName, Duration timeout, CompletableFuture<SegmentProperties> retVal) {
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getNfsRoot(), streamSegmentName),
                    PosixFileAttributes.class);
            StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName, attrs.size(), !(attrs.permissions().contains(OWNER_WRITE)), false,
                    new ImmutableDate(attrs.creationTime().toMillis()));

            retVal.complete(information);
        } catch (IOException e) {
            retVal.completeExceptionally(e);
        }
    }

    private void syncExists(String streamSegmentName, Duration timeout, CompletableFuture<Boolean> retFuture) {
        boolean exists = Files.exists(Paths.get(config.getNfsRoot(), streamSegmentName));
        retFuture.complete(exists);
    }


    private void syncCreate(String streamSegmentName, Duration timeout, CompletableFuture<SegmentProperties> retVal) {
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
            retVal.complete(this.getStreamSegmentInfo(streamSegmentName, timeout).get());
        } catch (Exception e) {
            if (e instanceof FileAlreadyExistsException) {
                retVal.completeExceptionally(new StreamSegmentExistsException(streamSegmentName, e));
            } else {
                retVal.completeExceptionally(e);
            }
        }
    }

    private void syncWrite(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout,
                           CompletableFuture<Void> retVal) {
        if (((NFSSegmentHandle) handle).channel == null || !Files.exists(
                Paths.get(config.getNfsRoot(), handle.getSegmentName()))) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
        } else {
            try {
                if (((NFSSegmentHandle) handle).channel.size() < offset) {
                    retVal.completeExceptionally(new BadOffsetException(handle.getSegmentName(), (
                            (NFSSegmentHandle) handle).channel.size(), offset));
                } else {
                    byte[] bytes = new byte[length];
                    try {
                        int read = data.read(bytes);
                        if ( read != length ) {
                            retVal.completeExceptionally(new IllegalArgumentException());
                        } else {
                            int bytesWritten = ((NFSSegmentHandle) handle).channel.write(ByteBuffer.wrap(bytes), offset);
                            ((NFSSegmentHandle) handle).channel.force(true);
                            retVal.complete(null);
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
            } catch (Exception exc) {
                if (exc instanceof NonWritableChannelException) {
                    retVal.completeExceptionally(new IllegalArgumentException(exc));
                } else if (exc instanceof ClosedChannelException) {
                    retVal.completeExceptionally(new StreamSegmentSealedException(handle.getSegmentName(), exc));
                } else {
                    retVal.completeExceptionally(exc);
                }
            }
        }
    }


    private void syncSeal(SegmentHandle handle, Duration timeout, CompletableFuture<Void> retVal) {
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
    }


    private void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout,
                            CompletableFuture<Void> retVal) {
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
                if (e instanceof NoSuchFileException || e instanceof FileNotFoundException) {
                    retVal.completeExceptionally(new StreamSegmentNotExistsException(targetHandle.getSegmentName()));
                } else {
                    retVal.completeExceptionally(e);
                }
            }
        }
    }


    private void syncDelete(SegmentHandle handle, Duration timeout, CompletableFuture<Void> future) {
        try {
            Files.delete(Paths.get(config.getNfsRoot(), handle.getSegmentName()));
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
    }

    //endregion

}
