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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.IllegalContainerStateException;
import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.StreamSegmentInformation;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.logs.CacheUpdater;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.writer.Writer;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
@Slf4j
class StreamSegmentContainer extends AbstractService implements SegmentContainer {
    //region Members

    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private final OperationLog durableLog;
    private final ReadIndex readIndex;
    private final Cache cache;
    private final Writer writer;
    private final Storage storage;
    private final PendingAppendsCollection pendingAppendsCollection;
    private final StreamSegmentMapper segmentMapper;
    private final Executor executor;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param metadataRepository       The MetadataRepository to use.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param readIndexFactory         The ReadIndexFactory to use to create Read Indices.
     * @param writerFactory            The WriterFactory to use to create Writers.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param cacheFactory             The CacheFactory to use to create Caches.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    StreamSegmentContainer(int streamSegmentContainerId, MetadataRepository metadataRepository, OperationLogFactory durableLogFactory, ReadIndexFactory readIndexFactory, WriterFactory writerFactory, StorageFactory storageFactory, CacheFactory cacheFactory, Executor executor) {
        Preconditions.checkNotNull(metadataRepository, "metadataRepository");
        Preconditions.checkNotNull(durableLogFactory, "durableLogFactory");
        Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        Preconditions.checkNotNull(writerFactory, "writerFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("SegmentContainer[%d]", streamSegmentContainerId);
        this.storage = storageFactory.getStorageAdapter();
        this.metadata = metadataRepository.getMetadata(streamSegmentContainerId);
        this.cache = cacheFactory.getCache(String.format("Container_%d", streamSegmentContainerId));
        this.readIndex = readIndexFactory.createReadIndex(this.metadata, this.cache);
        this.executor = executor;
        this.durableLog = durableLogFactory.createDurableLog(this.metadata, new CacheUpdater(this.cache, this.readIndex));
        this.durableLog.addListener(new ServiceShutdownListener(this.createComponentStoppedHandler("DurableLog"), this.createComponentFailedHandler("DurableLog")), this.executor);
        this.writer = writerFactory.createWriter(this.metadata, this.durableLog, this.cache);
        this.writer.addListener(new ServiceShutdownListener(this.createComponentStoppedHandler("Writer"), this.createComponentFailedHandler("Writer")), this.executor);
        this.pendingAppendsCollection = new PendingAppendsCollection();
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.storage, this.executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync().awaitTerminated();

            this.pendingAppendsCollection.close();
            this.writer.close();
            this.durableLog.close();
            this.readIndex.close();
            this.cache.close();
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStart");
        this.durableLog.startAsync();
        this.executor.execute(() -> {
            this.durableLog.awaitRunning();

            // DurableLog is running. Now start Writer.
            this.writer.startAsync();
            this.executor.execute(() -> {
                this.writer.awaitRunning();
                LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
                notifyStarted();
            });
        });
    }

    @Override
    protected void doStop() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStop");
        this.writer.stopAsync();
        this.durableLog.stopAsync();
        this.executor.execute(() -> {
            this.writer.awaitTerminated();
            this.durableLog.awaitTerminated();
            LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
            this.notifyStopped();
        });
    }

    //endregion

    //region Container Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
        ensureRunning();

        logRequest("append", streamSegmentName, data.length, appendContext);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, data, appendContext);
                    CompletableFuture<Long> result = this.durableLog.add(operation, timer.getRemaining());

                    // Add to Append Context Registry, if needed.
                    this.pendingAppendsCollection.register(operation, result);
                    return result.thenApply(seqNo -> operation.getStreamSegmentOffset());
                });
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureRunning();

        logRequest("read", streamSegmentName, offset, maxLength);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenApply(streamSegmentId -> this.readIndex.read(streamSegmentId, offset, maxLength, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("getStreamSegmentInfo", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenApply(streamSegmentId -> {
                    SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                    return new StreamSegmentInformation(streamSegmentName, sm.getDurableLogLength(), sm.isSealed(), sm.isDeleted(), new Date());
                });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("createStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewStreamSegment(streamSegmentName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamName, Duration timeout) {
        ensureRunning();

        logRequest("createBatch", parentStreamName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewBatchStreamSegment(parentStreamName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("deleteStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // metadata.deleteStreamSegment will delete the given StreamSegment and all batches associated with it.
        // It returns a collection of names of StreamSegments that were deleted.
        // As soon as this happens, all operations that deal with those segments will start throwing appropriate exceptions
        // or ignore the segments altogether (such as StorageWriter).
        Collection<String> streamSegmentsToDelete = this.metadata.deleteStreamSegment(streamSegmentName);
        CompletableFuture[] deletionFutures = new CompletableFuture[streamSegmentsToDelete.size()];
        int count = 0;
        for (String s : streamSegmentsToDelete) {
            deletionFutures[count] = this.storage.delete(s, timer.getRemaining());
            count++;
        }

        // Remove from Read Index.
        this.readIndex.performGarbageCollection();
        return CompletableFuture.allOf(deletionFutures);
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchStreamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("mergeBatch", batchStreamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(batchStreamSegmentName, timer.getRemaining())
                .thenCompose(batchStreamSegmentId -> {
                    SegmentMetadata batchMetadata = this.metadata.getStreamSegmentMetadata(batchStreamSegmentId);
                    if (batchMetadata == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(batchStreamSegmentName));
                    }

                    Operation op = new MergeBatchOperation(batchMetadata.getParentId(), batchMetadata.getId());
                    return this.durableLog.add(op, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("sealStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        AtomicReference<StreamSegmentSealOperation> operation = new AtomicReference<>();
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    operation.set(new StreamSegmentSealOperation(streamSegmentId));
                    return this.durableLog.add(operation.get(), timer.getRemaining());
                })
                .thenApply(seqNo -> operation.get().getStreamSegmentOffset());
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId, Duration timeout) {
        ensureRunning();

        logRequest("getLastAppendContext", streamSegmentName, clientId);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    CompletableFuture<AppendContext> result = this.pendingAppendsCollection.get(streamSegmentId, clientId);
                    if (result == null) {
                        // No appends pending for this StreamSegment/ClientId combination; check metadata.
                        SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                        if (segmentMetadata != null) {
                            result = CompletableFuture.completedFuture(segmentMetadata.getLastAppendContext(clientId));
                        }
                    }

                    return result;
                });
    }

    //endregion

    //region Helpers

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed, this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void logRequest(String requestName, Object... args) {
        log.info("{}: {} {}", this.traceObjectId, requestName, args);
    }

    private Consumer<Throwable> createComponentFailedHandler(String componentName) {
        return cause -> {
            log.warn("{}: DurableLog failed with exception {}", this.traceObjectId, cause);
            if (state() != State.STARTING) {
                // We cannot stop the service while we're starting it.
                stopAsync().awaitTerminated();
            }

            notifyFailed(cause);
        };
    }

    private Runnable createComponentStoppedHandler(String componentName) {
        return () -> {
            if (state() != State.STOPPING) {
                // The Queue Processor stopped but we are not in a stopping phase. We need to shut down right away.
                log.warn("{}: {} stopped unexpectedly (no error) but StreamSegmentContainer was not currently stopping. Shutting down StreamSegmentContainer.",
                        this.traceObjectId,
                        componentName);
                stopAsync().awaitTerminated();
            }
        };
    }

    //endregion
}
