/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.Timer;
import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 *  {@link io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider} for file system based storage.
 *
 * Each Chunk is represented as a single file on the underlying storage.
 * The concat operation is implemented as append.
 *
 */

@Slf4j
public class FileSystemChunkStorageProvider extends BaseChunkStorageProvider {
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

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemChunkStorageProvider class.
     *
     * @param config   The configuration to use.
     * @param executor Executor to use.
     */
    public FileSystemChunkStorageProvider(Executor executor, FileSystemStorageConfig config)  {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
    }

    //endregion

    //region

    @VisibleForTesting
    protected FileChannel getFileChannel(Path path, StandardOpenOption openOption) throws IOException {
        try {
            return FileChannel.open(path, openOption);
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @VisibleForTesting
    protected long getFileSize(Path path) throws IOException {
        try {
            return Files.size(path);
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException {
        try {
            PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getRoot(), chunkName),
                    PosixFileAttributes.class);
            ChunkInfo information = ChunkInfo.builder()
                    .name(chunkName)
                    .length(attrs.size())
                    //.lastModified(new ImmutableDate(attrs.creationTime().toMillis()))
                    .build();

            return information;
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(chunkName);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException {
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(READ_WRITE_PERMISSION);

        Path path = Paths.get(config.getRoot(), chunkName);
        Path parent = path.getParent();
        assert parent != null;
        Files.createDirectories(parent);
        Files.createFile(path, fileAttributes);
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean doesExist(String chunkName) throws IOException, IllegalArgumentException {
        return Files.exists(Paths.get(config.getRoot(), chunkName));
    }

    @Override
    protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException {
        Files.delete(Paths.get(config.getRoot(), handle.getChunkName()));
        return true;
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException {
        Path path = Paths.get(config.getRoot(), chunkName);

        if (!Files.exists(path)) {
            throw new FileNotFoundException(chunkName);
        }

        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException {
        Path path = Paths.get(config.getRoot(), chunkName);
        if (!Files.exists(path)) {
            throw new FileNotFoundException(chunkName);
        } else if (Files.isWritable(path)) {
            return ChunkHandle.writeHandle(chunkName);
        } else {
            return ChunkHandle.readHandle(chunkName);
        }
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        Timer timer = new Timer();

        Path path = Paths.get(config.getRoot(), handle.getChunkName());

        long fileSize = getFileSize(path);
        if (fileSize < fromOffset) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of chunk (%d).", fromOffset, fileSize));
        }

        try (FileChannel channel = getFileChannel(path, StandardOpenOption.READ)) {
            int totalBytesRead = 0;
            long readOffset = fromOffset;
            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                int bytesRead = channel.read(readBuffer, readOffset);
                bufferOffset += bytesRead;
                totalBytesRead += bytesRead;
                length -= bytesRead;
                readOffset += bytesRead;
            } while (length != 0);
            return totalBytesRead;
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IOException {
        Timer timer = new Timer();

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException("Write called on a readonly handle of chunk " + handle.getChunkName());
        }

        Path path = Paths.get(config.getRoot(), handle.getChunkName());

        long totalBytesWritten = 0;
        try (FileChannel channel = getFileChannel(path, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            if (fileSize != offset) {
                throw new IndexOutOfBoundsException(String.format("fileSize (%d) did not match offset (%d) for chunk %s", fileSize, offset, handle.getChunkName()));
            }

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
            channel.force(false);
        }
        return (int) totalBytesWritten;
    }

    @Override
    protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        int totalBytesConcated = 0;
        Path targetPath = Paths.get(config.getRoot(), target.getChunkName());
        for (ChunkHandle source: sources) {
            Preconditions.checkArgument(!target.getChunkName().equals(source.getChunkName()), "target and source can not be same.");
            Path sourcePath = Paths.get(config.getRoot(), source.getChunkName());
            long length = getFileSize(sourcePath);
            long offset = getFileSize(targetPath);
            try (FileChannel targetChannel = getFileChannel(targetPath, StandardOpenOption.WRITE);
                 RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
                while (length > 0) {
                    long bytesTransferred = targetChannel.transferFrom(sourceFile.getChannel(), offset, length);
                    offset += bytesTransferred;
                    length -= bytesTransferred;
                }
                targetChannel.force(false);
                Files.delete(sourcePath);
                totalBytesConcated += length;
            }
        }
        return totalBytesConcated;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        return false;
    }

    @Override
    protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException {
        Path path = null;
        try {
            path = Paths.get(config.getRoot(), handle.getChunkName());
            Files.setPosixFilePermissions(path, isReadOnly ? READ_ONLY_PERMISSION : READ_WRITE_PERMISSION);
            return true;
        } catch (NoSuchFileException e) {
            throw new FileNotFoundException(path.toString());
        }
    }
    //endregion
}
