/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.Executor;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

/***
 *  {@link io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider} for HDFS based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * The concat operation is implemented using HDFS native concat operation.
 */

@Slf4j
class HDFSChunkStorageProvider extends BaseChunkStorageProvider {
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    //region Members

    private final HDFSStorageConfig config;
    private FileSystem fileSystem;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     */
    HDFSChunkStorageProvider(Executor executor, HDFSStorageConfig config) {
        super(executor);
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        initialize();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.close();
                    this.fileSystem = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem: {}.", e);
                }
            }
        }
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        FileStatus last = fileSystem.getFileStatus(getFilePath(chunkName));
        ChunkInfo result = ChunkInfo.builder().name(chunkName).length(last.getLen()).build();
        return result;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        try {
            Path fullPath = getFilePath(chunkName);
            // Create the file, and then immediately close the returned OutputStream, so that HDFS may properly create the file.
            this.fileSystem.create(fullPath, READWRITE_PERMISSION, false, 0, this.config.getReplication(),
                    this.config.getBlockSize(), null).close();
            log.debug("Created '{}'.", fullPath);

            // return handle
            return ChunkHandle.writeHandle(chunkName);
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e ) {
            throw new FileAlreadyExistsException(chunkName);
        }
    }

    @Override
    protected boolean doesExist(String chunkName) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        FileStatus status = null;
        try {
            status = fileSystem.getFileStatus(getFilePath(chunkName));
        } catch (IOException e) {
            // HDFS could not find the file. Returning false.
            log.warn("Got exception checking if file exists", e);
        }
        boolean exists = status != null;
        return exists;
    }

    @Override
    protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        boolean retValue = this.fileSystem.delete(getFilePath(handle.getChunkName()), true);
        return retValue;
    }


    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        FileStatus status = fileSystem.getFileStatus(getFilePath(chunkName));
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException {
        ensureInitializedAndNotClosed();
        FileStatus status = fileSystem.getFileStatus(getFilePath(chunkName));
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        ensureInitializedAndNotClosed();

        if (fromOffset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    fromOffset, bufferOffset, length, buffer.length));
        }

        int totalBytesRead  = readInternal(handle, buffer, fromOffset, bufferOffset, length);
        return totalBytesRead;

    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IOException, IndexOutOfBoundsException {
        ensureInitializedAndNotClosed();
        try (FSDataOutputStream stream = this.fileSystem.append(getFilePath(handle.getChunkName()))) {
            if (stream.getPos() != offset) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                throw new IndexOutOfBoundsException();
            }

            if (length == 0) {
                // Exit here (vs at the beginning of the method), since we want to throw appropriate exceptions in case
                // of Sealed or BadOffset
                // Note: IOUtils.copyBytes with length == 0 will enter an infinite loop, hence the need for this check.
                return 0;
            }

            // We need to be very careful with IOUtils.copyBytes. There are many overloads with very similar signatures.
            // There is a difference between (InputStream, OutputStream, int, boolean) and (InputStream, OutputStream, long, boolean),
            // in that the one with "int" uses the third arg as a buffer size, and the one with "long" uses it as the number
            // of bytes to copy.
            IOUtils.copyBytes(data, stream, (long) length, false);

            stream.flush();
        } finally {

        }
        return length;
    }

    @Override
    protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        ensureInitializedAndNotClosed();
        // Concat source file into target.
        this.fileSystem.concat(getFilePath(target.getChunkName()), new Path[]{getFilePath(sources[0].getChunkName())});
        return 0;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException(getClass().getName() + " does not support chunk truncation.");
    }

    @Override
    protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException {
        this.fileSystem.setPermission(getFilePath(handle.getChunkName()), isReadOnly ? READONLY_PERMISSION : READWRITE_PERMISSION);
        return true;
    }

    //endregion

    //region Storage Implementation

    @SneakyThrows(IOException.class)
    public void initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", this.config.getHdfsHostURL());
        conf.set("fs.default.fs", this.config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // FileSystem has a bad habit of caching Clients/Instances based on target URI. We do not like this, since we
        // want to own our implementation so that when we close it, we don't interfere with others.
        conf.set("fs.hdfs.impl.disable.cache", "true");
        if (!this.config.isReplaceDataNodesOnFailure()) {
            // Default is DEFAULT, so we only set this if we want it disabled.
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        }

        this.fileSystem = openFileSystem(conf);
        log.info("Initialized (HDFSHost = '{}'", this.config.getHdfsHostURL());
    }

    FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }


    @Override
    public boolean supportsTruncation() {
        ensureInitializedAndNotClosed();
        return false;
    }
    //endregion

    //region Helpers
    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //Region HDFS helper methods.

    /**
     * Gets an HDFS-friendly path prefix for the given chunk name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String chunkName) {
        return this.config.getHdfsRoot() + Path.SEPARATOR + chunkName;
    }

    /**
     * Gets the full HDFS Path to a file for the given chunk, startOffset and epoch.
     */
    private Path getFilePath(String chunkName) {
        Preconditions.checkState(chunkName != null && chunkName.length() > 0, "chunkName must be non-null and non-empty");
        return new Path(getPathPrefix(chunkName));
    }

    /**
     * Determines whether the given FileStatus indicates the file is read-only.
     *
     * @param fs The FileStatus to check.
     * @return True or false.
     */
    private boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    private int readInternal(ChunkHandle handle, byte[] buffer, long offset, int bufferOffset, int length) throws IOException {
        //There is only one file per chunkName.
        try (FSDataInputStream stream = this.fileSystem.open(getFilePath(handle.getChunkName()))) {
            stream.readFully(offset, buffer, bufferOffset, length);
        } catch (EOFException e) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the current size of chunk.", offset));
        }
        return length;
    }

    //endregion
}