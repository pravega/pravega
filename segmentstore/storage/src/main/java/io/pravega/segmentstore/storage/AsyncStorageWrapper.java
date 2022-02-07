/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.contracts.SegmentProperties;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Wrapper for a SyncStorage implementation that executes all operations asynchronously in a Thread Pool.
 *
 * Instances of this class guarantee that no two operations on the same Segment can execute concurrently in the same
 * instance. Different Segments are not affected.
 *
 */
@ThreadSafe
public class AsyncStorageWrapper implements Storage {
    //region Members

    private final SyncStorage syncStorage;
    private final Executor executor;
    private final MultiKeySequentialProcessor<String> taskProcessor;
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
        this.taskProcessor = new MultiKeySequentialProcessor<>(this.executor);
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
        return supplyAsync(() -> this.syncStorage.openWrite(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.create(streamSegmentName, rollingPolicy), streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(() -> this.syncStorage.write(handle, offset, data, length), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return runAsync(() -> this.syncStorage.seal(handle), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return runAsync(() -> this.syncStorage.concat(targetHandle, offset, sourceSegment), targetHandle.getSegmentName(), sourceSegment);
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return runAsync(() -> this.syncStorage.delete(handle), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        return runAsync(() -> this.syncStorage.truncate(handle, offset), handle.getSegmentName());
    }

    @Override
    public boolean supportsTruncation() {
        return this.syncStorage.supportsTruncation();
    }

    @Override
    public boolean supportsAtomicWrites() {
        // RollingStorage (non-Chunked Storage) does not support atomic writes if the underlying Storage implementation does not.
        return false;
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(() -> this.syncStorage.openRead(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.read(handle, offset, buffer, bufferOffset, length), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.getStreamSegmentInfo(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> this.syncStorage.exists(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Iterator<SegmentProperties>> listSegments() {
        try {
            return CompletableFuture.completedFuture(this.syncStorage.listSegments());
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    //endregion

    //region Helpers

    /**
     * Gets a value representing the number of segments that currently have at least an ongoing task running.
     */
    @VisibleForTesting
    int getSegmentWithOngoingOperationsCount() {
        return this.taskProcessor.getCurrentTaskCount();
    }

    /**
     * Executes the given Callable asynchronously and returns a CompletableFuture that will be completed with the result.
     * @param operation    The Callable to execute.
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     */
    private <R> CompletableFuture<R> supplyAsync(Callable<R> operation, String... segmentNames) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.taskProcessor.add(Arrays.asList(segmentNames), () -> execute(operation));
    }

    /**
     * Executes the given RunnableWithException asynchronously and returns a CompletableFuture that will be completed
     * when the Runnable completes.
     * @param operation    The RunnableWithException to execute.
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     */
    private CompletableFuture<Void> runAsync(RunnableWithException operation, String... segmentNames) {
        return supplyAsync(() -> {
            operation.run();
            return null;
        }, segmentNames);
    }

    /**
     * Executes the given Callable synchronously and invokes cleanup when done.
     *
     * @param operation    The Callable to execute.
     */
    private <R> CompletableFuture<R> execute(Callable<R> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return operation.call();
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, this.executor);
    }

    //endregion
}
