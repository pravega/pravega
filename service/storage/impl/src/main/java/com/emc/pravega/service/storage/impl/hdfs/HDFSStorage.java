/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamingException;
import com.emc.pravega.service.storage.InvalidSegmentHandleException;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageException;
import com.emc.pravega.service.storage.StorageSegmentInformation;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HDFS Storage adapter which mounts to an HDFS filesystem.
 * Each Segment is represented by a file with pattern <segment-name>_<owner_host_id>
 * Owner_host_id is optional and means that the segment is owned by the Pravega host with the given id (from configuration).
 * <p>
 * Whenever ownership change happens, the new node renames the file representing the segment to <segment-name>_<owner_host_id>.
 * This is done by the open() method, which returns a segment-specific handle.
 * <p>
 * When a segment is sealed, it is renamed to its absolute name "segment-name" and marked as read-only.
 */
@Slf4j
class HDFSStorage implements Storage {
    //region Members

    private static final FsPermission NORMAL_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission SEALED_PERMISSION = new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE);
    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;
    private final AtomicBoolean closed;
    private FileSystem fileSystem;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param serviceBuilderConfig The configuration to use.
     * @param executor             The Executor to use.
     */
    HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        Preconditions.checkNotNull(executor, "executor");
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor = executor;
        this.closed = new AtomicBoolean();
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

    //endregion

    //region Initialization

    /**
     * Initializes the HDFSStorage.
     *
     * @throws IOException If the initialization failed.
     */
    public void initialize() throws IOException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", serviceBuilderConfig.getHDFSHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.fileSystem = FileSystem.get(conf);
        this.closed.set(false);
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout) {
        HDFSSegmentHandle handle = new HDFSSegmentHandle(streamSegmentName, getOwnedSegmentFullPath(streamSegmentName));
        return supplyAsync(() -> createSync(handle), streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentHandle> open(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> openSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(() -> writeSync(getHandle(handle), offset, length, data), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(SegmentHandle handle, Duration timeout) {
        return supplyAsync(() -> sealSync(getHandle(handle)), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, SegmentHandle sourceHandle, Duration timeout) {
        return runAsync(() -> concatSync(getHandle(targetHandle), offset, getHandle(sourceHandle)), targetHandle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return runAsync(() -> deleteSync(getHandle(handle)), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(() -> readSync(getHandle(handle), offset, buffer, bufferOffset, length), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<StorageSegmentInformation> getStreamSegmentInfo(String segmentName, Duration timeout) {
        return supplyAsync(() -> {
            HDFSSegmentHandle handle = openSync(segmentName);
            SegmentProperties props = getStreamSegmentInfoSync(handle);
            return new StorageSegmentInformation(handle, props.getLength(), props.isSealed(), props.isDeleted(), props.getLastModified());
        }, segmentName);
    }

    @Override
    public CompletableFuture<Boolean> exists(String segmentName, Duration timeout) {
        return supplyAsync(() -> existsSync(segmentName), segmentName);
    }

    //endregion

    //region HDFS Client wrappers

    private SegmentHandle createSync(HDFSSegmentHandle handle) throws IOException {
        // Create an immediately close the returned stream - we do not need it.
        this.fileSystem.create(handle.getPhysicalSegmentPath(),
                NORMAL_PERMISSION,
                false,
                0,
                this.serviceBuilderConfig.getReplication(),
                this.serviceBuilderConfig.getBlockSize(),
                null).close();

        return handle;
    }

    private SegmentProperties getStreamSegmentInfoSync(HDFSSegmentHandle handle) throws IOException {
        FileStatus[] status = this.fileSystem.globStatus(handle.getPhysicalSegmentPath());
        return new StreamSegmentInformation(handle.getSegmentName(),
                status[0].getLen(),
                status[0].getPermission().getUserAction() == FsAction.READ,
                false,
                new Date(status[0].getModificationTime()));
    }

    private void writeSync(HDFSSegmentHandle handle, long offset, int length, InputStream data) throws Exception {
        val status = getStatus(handle);
        checkNotSealed(status, handle);
        try (FSDataOutputStream stream = this.fileSystem.append(handle.getPhysicalSegmentPath())) {
            if (stream.getPos() != offset) {
                throw new BadOffsetException(handle.getSegmentName(), offset, stream.getPos());
            }

            // Copy the bytes and close the stream. This will automatically call flush.
            IOUtils.copyBytes(data, stream, length);
        }
    }

    private SegmentProperties sealSync(HDFSSegmentHandle handle) throws Exception {
        val status = getStatus(handle);
        checkNotSealed(status, handle);
        this.fileSystem.setPermission(handle.getPhysicalSegmentPath(), SEALED_PERMISSION);
        return getStreamSegmentInfoSync(handle);
    }

    private void concatSync(HDFSSegmentHandle targetSegmentHandle, long offset, HDFSSegmentHandle sourceSegmentHandle) throws Exception {
        // Verify target segment seal status and current offset.
        val targetStatus = getStatus(targetSegmentHandle);
        checkNotSealed(targetStatus, targetSegmentHandle);
        if (targetStatus.getLen() != offset) {
            throw new BadOffsetException(targetSegmentHandle.getSegmentName(), offset, targetStatus.getLen());
        }

        // Verify source segment seal status.
        val sourceStatus = getStatus(sourceSegmentHandle);
        if (sourceStatus.getPermission().getUserAction().implies(FsAction.WRITE)) {
            throw new IllegalStateException(String.format("Cannot concat segment '%s' into '%s' because the source segment is not sealed.", sourceSegmentHandle.getSegmentName(), targetSegmentHandle.getSegmentName()));
        }

        this.fileSystem.concat(
                targetSegmentHandle.getPhysicalSegmentPath(),
                new Path[]{sourceSegmentHandle.getPhysicalSegmentPath()});
    }

    private int readSync(HDFSSegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws Exception {
        Preconditions.checkArgument(offset >= 0, "offset", "offset must be a non-negative number.");
        Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

        int bytesRead;
        try (FSDataInputStream stream = this.fileSystem.open(handle.getPhysicalSegmentPath())) {
            bytesRead = stream.read(offset, buffer, bufferOffset, length);
        }

        if (bytesRead < 0) {
            // -1 is usually a code for invalid args; check to see if we were supplied with an offset that exceeds the length of the segment.
            val status = getStatus(handle);
            if (offset >= status.getLen()) {
                throw new IllegalArgumentException(String.format("Read offset (%s) is beyond the length of the segment (%s).", offset, status.getLen()));
            }
        }
        return bytesRead;
    }

    private boolean existsSync(String segmentName) throws IOException {
        FileStatus[] statuses = this.fileSystem.globStatus(new Path(getCommonPartOfName(segmentName) + "_[0-9]*"));
        return statuses.length == 1;
    }

    private void deleteSync(HDFSSegmentHandle handle) throws IOException {
        this.fileSystem.delete(handle.getPhysicalSegmentPath(), false);
    }

    /**
     * Attempts to take over ownership of a segment.
     * <ol>
     * <li> Find the file with the biggest start offset.
     * <li> Create a new file with the name equal to the current offset of the stream which is biggest start offset + its size.
     * </ol>
     */
    private HDFSSegmentHandle openSync(String streamSegmentName) throws IOException, StreamSegmentNotExistsException {
        FileStatus[] statuses = this.fileSystem.globStatus(new Path(getCommonPartOfName(streamSegmentName) + "_[0-9]*"));

        if (statuses.length != 1) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        HDFSSegmentHandle handle = new HDFSSegmentHandle(streamSegmentName, getOwnedSegmentFullPath(streamSegmentName));
        this.fileSystem.rename(statuses[0].getPath(), handle.getPhysicalSegmentPath());
        return handle;
    }

    private void checkNotSealed(FileStatus status, HDFSSegmentHandle handle) throws StreamSegmentSealedException {
        if (!status.getPermission().getUserAction().implies(FsAction.WRITE)) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }
    }

    private FileStatus getStatus(HDFSSegmentHandle handle) throws IOException, StreamSegmentNotExistsException {
        val status = this.fileSystem.getFileStatus(handle.getPhysicalSegmentPath());
        if (status == null) {
            throw new StreamSegmentNotExistsException(handle.getSegmentName());
        }

        return status;
    }

    //endregion

    //region Helpers

    /**
     * Gets the full name of the file representing the segment which is locked by the current Pravega host.
     */
    private String getOwnedSegmentFullPath(String streamSegmentName) {
        return getCommonPartOfName(streamSegmentName) + "_" + this.serviceBuilderConfig.getPravegaId();
    }

    private String getCommonPartOfName(String streamSegmentName) {
        return this.serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName;
    }

    @SneakyThrows(Throwable.class)
    private Exception toStreamingException(String streamSegmentName, Throwable e) {
        if ((e instanceof PathNotFoundException) || (e instanceof FileNotFoundException)) {
            e = new StreamSegmentNotExistsException(streamSegmentName, e);
        } else if (e instanceof FileAlreadyExistsException) {
            e = new StreamSegmentExistsException(streamSegmentName, e);
        } else if (e instanceof AclException) {
            e = new StreamSegmentSealedException(streamSegmentName, e);
        } else if (!isPassThrough(e)) {
            e = new StorageException("General error while performing HDFS operation.", e);
        } else if (!(e instanceof Exception)) {
            throw e;
        }

        return (Exception) e;
    }

    private boolean isPassThrough(Throwable ex) {
        return ex instanceof StreamingException || ex instanceof RuntimeException;
    }

    private HDFSSegmentHandle getHandle(SegmentHandle handle) {
        // TODO: generalize this as a checked cast in the 'common' package.
        Preconditions.checkNotNull(handle, "handle");
        if (!(handle instanceof HDFSSegmentHandle)) {
            throw new InvalidSegmentHandleException(handle);
        }

        HDFSSegmentHandle ourHandle = (HDFSSegmentHandle) handle;
        if (ourHandle.getPhysicalSegmentPath() == null) {
            throw new InvalidSegmentHandleException(handle);
        }

        return ourHandle;
    }

    private CompletableFuture<Void> runAsync(RunnableWithException syncCode, String streamSegmentName) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            try {
                syncCode.run();
            } catch (Exception e) {
                throw new CompletionException(toStreamingException(streamSegmentName, e));
            }
        }, this.executor);
    }

    private <T> CompletableFuture<T> supplyAsync(Callable<T> syncCode, String streamSegmentName) {
        ensureInitializedAndNotClosed();

        // TODO: both this method and runAsync are quite ugly. See if we can extract a pattern from both where we don't need to wrap the exception.
        return CompletableFuture.supplyAsync(() -> {
            try {
                return syncCode.call();
            } catch (Exception e) {
                throw new CompletionException(toStreamingException(streamSegmentName, e));
            }
        }, this.executor);
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion
}
