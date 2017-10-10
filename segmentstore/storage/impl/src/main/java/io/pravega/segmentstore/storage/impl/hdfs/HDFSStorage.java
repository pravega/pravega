/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Storage adapter for a backing HDFS Store which implements fencing using file-chaining strategy.
 * <p>
 * Each segment is a collection of ordered files, adopting the following pattern: {segment-name}_{start-offset}_{epoch}.
 * <ul>
 * <li> {segment-name} is the name of the segment as used in the SegmentStore
 * <li> {start-offset} is the offset within the Segment of the first byte in this file.
 * <li> {epoch} is the Container Epoch which had ownership of that segment when that file was created.
 * </ul>
 * <p>
 * Example: Segment "foo" can have these files
 * <ol>
 * <li> foo_0_1: first file, contains data at offsets [0..1234), written during Container Epoch 1.
 * <li> foo_1234_2: data at offsets [1234..987632), written during Container Epoch 2.
 * <li> foo_987632_3: data at offsets [987632..10568949), written during Container Epoch 3.
 * <li> foo_10568949_4: data at offsets [10568949..current-length-of-segment), written during Container Epoch 4.
 * </ol>
 * <p>
 * All but the last file is marked as 'read-only' (except if the segment is Sealed, in which case the last file too is
 * marked as 'read-only').
 * <p>
 * When a container fails over and needs to reacquire ownership of a segment, it marks the current active file as read-only
 * and opens a new one (which becomes the new active). Small variations from this rule may exist, such as whether the last
 * file is empty or not (if empty, it may make more sense to delete it).
 * Example: using the same file names as above, after writing 1234 bytes we failed over, then again after writing 987632
 * and again after writing 10568949. When the last failover happened, we have the following state:
 * <ol>
 * <li> foo_0_1: read-only.
 * <li> foo_1234_2: read-only.
 * <li> foo_987632_3: read-only.
 * <li> foo_10568949_4: not read-only (this is the active file).
 * </ol>
 * <p>
 * When acquiring ownership, the Container create a new "active" file by calling openWrite() and it should only try to
 * modify that file. It should never try to create a new one or re-acquire ownership.
 * <p>
 * When a failover happens, the previous Container (if still active) will detect that its file has become read-only or
 * deleted (via a failed write) and know it's time to stop all activity for that segment (i.e., it was fenced out).
 * <p>
 * In order to resolve disputes (so that there is a clear protocol as to which instance of a Container will end up owning
 * the segment should there be more than one fighting for one), the Container Epoch is used. Containers with higher epochs
 * will win over Containers with lower epochs. For example, if two different instances of the same Container (one with
 * epoch 1 and one with epoch 2) try to create segment "foo" at the exact same time, then they will create "foo_0_1" and
 * "foo_0_2". Then they will detect that there is more than one file, and the Container with epoch 1 will back out and
 * yield to the outcome of the Container with Epoch 2. A similar approach is taken for all other operations, with more
 * details in the code for each.
 */
@Slf4j
class HDFSStorage implements SyncStorage {
    //region Members

    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private FileSystemOperation.OperationContext context;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     */
    HDFSStorage(HDFSStorageConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.context != null) {
                try {
                    this.context.fileSystem.close();
                    this.context = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem: {}.", e);
                }
            }
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    @SneakyThrows(IOException.class)
    public void initialize(long epoch) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.context == null, "HDFSStorage has already been initialized.");
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number. Given %s.", epoch);
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

        this.context = new FileSystemOperation.OperationContext(epoch, openFileSystem(conf), this.config);
        log.info("Initialized (HDFSHost = '{}', Epoch = {}).", this.config.getHdfsHostURL(), epoch);
    }

    protected FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
        return call(new CreateOperation(streamSegmentName, this.context));
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        return call(new OpenWriteOperation(streamSegmentName, this.context));
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        return call(new OpenReadOperation(streamSegmentName, this.context));
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        run(new WriteOperation(asWritableHandle(handle), offset, data, length, this.context));
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        run(new SealOperation(asWritableHandle(handle), this.context));
    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
        run(new ConcatOperation(asWritableHandle(targetHandle), offset, sourceSegment, this.context));
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        run(new DeleteOperation(asReadableHandle(handle), this.context));
    }


    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }


    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        return call(new ReadOperation(asReadableHandle(handle), offset, buffer, bufferOffset, length, this.context));
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        return call(new GetInfoOperation(streamSegmentName, this.context));
    }

    @Override
    @SneakyThrows(StreamSegmentException.class)
    public boolean exists(String streamSegmentName) {
        return call(new ExistsOperation(streamSegmentName, this.context));
    }

    //endregion

    //region Helpers

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     */
    private <T extends FileSystemOperation & RunnableWithException> void run(T operation) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        try {
            operation.run();
        } catch (Exception e) {
            throwException(e, operation);
        }
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     * @param <R>       Return type of the operation.
     * @return Instance of the return type of the operation.
     */
    private <R, T extends FileSystemOperation & Callable<? extends R>> R call(T operation) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        try {
            return operation.call();
        } catch (Exception e) {
            return throwException(e, operation);
        }
    }


    private <T> T throwException(Throwable e, FileSystemOperation<?> operation) throws StreamSegmentException {
        String segmentName = operation.getTarget() instanceof SegmentHandle
                ? ((SegmentHandle) operation.getTarget()).getSegmentName()
                : operation.getTarget().toString();
        return HDFSExceptionHelpers.throwException(segmentName, e);
    }

    /**
     * Casts the given handle as a HDFSSegmentHandle that has isReadOnly == false.
     */
    private HDFSSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    /**
     * Casts the given handle as a HDFSSegmentHandle irrespective of its isReadOnly value.
     */
    private HDFSSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof HDFSSegmentHandle, "handle must be of type HDFSSegmentHandle.");
        return (HDFSSegmentHandle) handle;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.context != null, "HDFSStorage is not initialized.");
    }

    //endregion
}
