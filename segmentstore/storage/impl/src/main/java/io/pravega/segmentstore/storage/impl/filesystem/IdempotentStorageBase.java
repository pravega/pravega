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
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.Storage;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for all the storage implementations that support idempotent write at an offset.
 */
public abstract class IdempotentStorageBase implements Storage {

    //region members

    private final ExecutorService executor;
    private final AtomicBoolean closed;

    //endregion

    //region constructor

    public IdempotentStorageBase(ExecutorService executor) {
        Preconditions.checkNotNull(executor, "executor");
        this.executor = executor;
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region common methods

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     *
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
    }

    /**
     * Executes the given supplier asynchronously and returns a Future that will be completed with the result.
     */
    protected <R> CompletableFuture<R> supplyAsync(String segmentName, Callable<R> operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                handleException(e, segmentName, result);
            }
        });

        return result;
    }

    private <R> void handleException(Throwable e, String segmentName, CompletableFuture<R> result) {
        result.completeExceptionally(translateException(segmentName, e));
    }

    //endregion

    //region abstract methods

    protected abstract Throwable translateException(String segmentName, Throwable e);

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion
}
