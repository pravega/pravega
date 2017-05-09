/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.common.function.RunnableWithException;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.storage.SegmentHandle;
import io.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
class HDFSStorage implements Storage {
    //region Members

    private final Executor executor;
    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private FileSystemOperation.OperationContext context;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     * @param executor The executor to use for running async operations.
     */
    HDFSStorage(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
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
        this.context = new FileSystemOperation.OperationContext(epoch, openFileSystem(conf), this.config);
        log.info("Initialized (HDFSHost = '{}', Epoch = {}).", this.config.getHdfsHostURL(), epoch);
    }

    protected FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(new CreateOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return supplyAsync(new OpenWriteOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(new OpenReadOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(new WriteOperation(asWritableHandle(handle), offset, data, length, this.context));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return runAsync(new SealOperation(asWritableHandle(handle), this.context));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return runAsync(new ConcatOperation(asWritableHandle(targetHandle), offset, sourceSegment, this.context));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return runAsync(new DeleteOperation(asReadableHandle(handle), this.context));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(new ReadOperation(asReadableHandle(handle), offset, buffer, bufferOffset, length, this.context));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(new GetInfoOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(new ExistsOperation(streamSegmentName, this.context));
    }

    //endregion

    //region Helpers

    /**
     * Executes the given FileSystemOperation asynchronously and returns a Future that will be completed when it finishes.
     */
    private <T extends FileSystemOperation & RunnableWithException> CompletableFuture<Void> runAsync(T operation) {
        ensureInitializedAndNotClosed();
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                operation.run();
                result.complete(null);
            } catch (Throwable e) {
                handleException(e, operation, result);
            }
        });

        return result;
    }

    /**
     * Executes the given FileSystemOperation asynchronously and returns a Future that will be completed with the result.
     */
    private <R, T extends FileSystemOperation & Callable<? extends R>> CompletableFuture<R> supplyAsync(T operation) {
        ensureInitializedAndNotClosed();
        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                handleException(e, operation, result);
            }
        });

        return result;
    }

    private void handleException(Throwable e, FileSystemOperation<?> operation, CompletableFuture<?> result) {
        String segmentName = operation.getTarget() instanceof SegmentHandle
                ? ((SegmentHandle) operation.getTarget()).getSegmentName()
                : operation.getTarget().toString();
        result.completeExceptionally(HDFSExceptionHelpers.translateFromException(segmentName, e));
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
