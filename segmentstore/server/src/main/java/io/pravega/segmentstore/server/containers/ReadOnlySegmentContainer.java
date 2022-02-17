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
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReader;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.StorageFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A slimmed down version of StreamSegmentContainer that is only able to perform reads from Storage. This SegmentContainer
 * cannot make any modifications to any Segments, nor can it create new or delete existing ones. It also cannot access data
 * that exists solely in DurableDataLog (which has not yet been transferred into permanent Storage).
 */
@Slf4j
class ReadOnlySegmentContainer extends AbstractIdleService implements SegmentContainer {
    //region Members
    @VisibleForTesting
    static final int MAX_READ_AT_ONCE_BYTES = 4 * 1024 * 1024;
    private static final int CONTAINER_ID = Integer.MAX_VALUE; // So that it doesn't collide with any other real Container Id.
    private static final int CONTAINER_EPOCH = 1; // This guarantees that any write operations should be fenced out if attempted.
    private static final Retry.RetryAndThrowExceptionally<StreamSegmentNotExistsException, RuntimeException> READ_RETRY = Retry
            .withExpBackoff(30, 10, 4)
            .retryingOn(StreamSegmentNotExistsException.class)
            .throwingOn(RuntimeException.class);
    private final ReadOnlyStorage storage;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadOnlySegmentContainer class.
     *
     * @param storageFactory A StorageFactory used to create Storage adapters.
     * @param executor       An Executor to use for async operations.
     */
    ReadOnlySegmentContainer(StorageFactory storageFactory, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.storage = storageFactory.createStorageAdapter();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            Futures.await(Services.stopAsync(this, this.executor));
            this.storage.close();
            log.info("Closed.");
        }
    }

    //endregion

    //region AbstractIdleService Implementation

    @Override
    protected Executor executor() {
        return this.executor;
    }

    @Override
    protected void startUp() {
        this.storage.initialize(CONTAINER_EPOCH);
        log.info("Started.");
    }

    @Override
    protected void shutDown() {
        log.info("Stopped.");
    }

    //endregion

    // SegmentContainer Implementation

    @Override
    public int getId() {
        return CONTAINER_ID;
    }

    @Override
    public boolean isOffline() {
        // ReadOnlySegmentContainer is always online.
        return false;
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return READ_RETRY.run(() -> getStreamSegmentInfo(streamSegmentName, timer.getRemaining())
                .thenApply(si -> StreamSegmentStorageReader.read(si, offset, maxLength, MAX_READ_AT_ONCE_BYTES, this.storage)));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return READ_RETRY.run(() -> this.storage.getStreamSegmentInfo(streamSegmentName, timeout));
    }

    //endregion

    //region Unsupported Operations

    @Override
    public Collection<SegmentProperties> getActiveSegments() {
        throw new UnsupportedOperationException("getActiveSegments is not supported on " + getClass().getSimpleName());
    }

    @Override
    public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
        throw new UnsupportedOperationException("getExtension is not supported on " + getClass().getSimpleName());
    }

    @Override
    public CompletableFuture<Void> flushToStorage(Duration timeout) {
        throw new UnsupportedOperationException("flushToStorage is not supported on " + getClass().getSimpleName());
    }

    @Override
    public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
        throw new UnsupportedOperationException("getExtendedChunkInfo is not supported on " + getClass().getSimpleName());
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return unsupported("append");
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return unsupported("append");
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return unsupported("updateAttributes");
    }

    @Override
    public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
        return unsupported("getAttributes");
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType, Collection<AttributeUpdate> attributes, Duration timeout) {
        return unsupported("createStreamSegment");
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        return unsupported("mergeStreamSegment");
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment,
                                                                          AttributeUpdateCollection attributes, Duration timeout) {
        return unsupported("mergeStreamSegment");
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        return unsupported("sealStreamSegment");
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        return unsupported("deleteStreamSegment");
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        return unsupported("truncateStreamSegment");
    }

    @Override
    public CompletableFuture<DirectSegmentAccess> forSegment(String streamSegmentName, @Nullable OperationPriority priority, Duration timeout) {
        return unsupported("forSegment");
    }

    private <T> CompletableFuture<T> unsupported(String methodName) {
        return Futures.failedFuture(new UnsupportedOperationException(methodName + " is unsupported on " + getClass().getSimpleName()));
    }

    //endregion
}
