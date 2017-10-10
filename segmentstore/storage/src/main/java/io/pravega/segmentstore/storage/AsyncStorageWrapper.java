/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for a SyncStorage implementation that executes all operations asynchronously in a Thread Pool.
 */
public class AsyncStorageWrapper implements Storage {
    //region Members

    private final SyncStorage syncStorage;
    private final Executor executor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncStorageWrapper class.
     *
     * @param syncStorage A SyncStorage instance that will be wrapped.
     * @param executor    An Executor for async operations.
     */
    public AsyncStorageWrapper(SyncStorage syncStorage, Executor executor) {
        this.syncStorage = Preconditions.checkNotNull(syncStorage, "syncStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.syncStorage.close();
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public void initialize(long containerEpoch) {
        this.syncStorage.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return supplyAsync(() -> this.syncStorage.openWrite(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.create(streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(() -> this.syncStorage.write(handle, offset, data, length));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return runAsync(() -> this.syncStorage.seal(handle));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return runAsync(() -> this.syncStorage.concat(targetHandle, offset, sourceSegment));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return runAsync(() -> this.syncStorage.delete(handle));
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        return runAsync(() -> this.syncStorage.truncate(handle, offset));
    }

    @Override
    public boolean supportsTruncation() {
        return this.syncStorage.supportsTruncation();
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(() -> this.syncStorage.openRead(streamSegmentName));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.read(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.getStreamSegmentInfo(streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.exists(streamSegmentName));
    }

    //endregion

    //region Helpers

    /**
     * Executes the given Supplier asynchronously and returns a CompletableFuture that will be completed with the result.
     */
    private <R> CompletableFuture<R> supplyAsync(Callable<R> operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    /**
     * Executes the given RunnableWithException asynchronously and returns a CompletableFuture that will be completed
     * when the Runnable completes.
     */
    private CompletableFuture<Void> runAsync(RunnableWithException operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                operation.run();
                result.complete(null);
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    //endregion
}
