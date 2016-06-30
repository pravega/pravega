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

package com.emc.logservice.server.logs;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.common.LoggerHelpers;
import com.emc.logservice.common.TimeoutTimer;
import com.emc.logservice.contracts.StreamSegmentException;
import com.emc.logservice.contracts.StreamingException;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.IllegalContainerStateException;
import com.emc.logservice.server.LogItemFactory;
import com.emc.logservice.server.ServiceShutdownListener;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.server.logs.operations.MetadataOperation;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.OperationFactory;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents an OperationLog that durably stores Log Operations it receives.
 */
@Slf4j
public class DurableLog extends AbstractService implements OperationLog {
    //region Members

    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(30);
    private final String traceObjectId;
    private final LogItemFactory<Operation> operationFactory;
    private final MemoryOperationLog inMemoryOperationLog;
    private final DurableDataLog dataFrameLog;
    private final MemoryLogUpdater memoryLogUpdater;
    private final OperationProcessor operationProcessor;
    private final UpdateableContainerMetadata metadata;
    private final TruncationMarkerCollection truncationMarkers;
    private final Executor executor;
    private final AtomicReference<Throwable> stopException = new AtomicReference<>();
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLog class.
     *
     * @param metadata            The StreamSegment Container Metadata for the container which this Durable Log is part of.
     * @param dataFrameLogFactory A DurableDataLogFactory which can be used to create instances of DataFrameLogs.
     * @param cache               An Cache where to store newly processed appends.
     * @throws NullPointerException If any of the arguments are null.
     */
    public DurableLog(UpdateableContainerMetadata metadata, DurableDataLogFactory dataFrameLogFactory, Cache cache, Executor executor) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(dataFrameLogFactory, "dataFrameLogFactory");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(executor, "executor");

        this.dataFrameLog = dataFrameLogFactory.createDurableDataLog(metadata.getContainerId());
        assert this.dataFrameLog != null : "dataFrameLogFactory created null dataFrameLog.";

        this.traceObjectId = String.format("DurableLog[%s]", metadata.getContainerId());
        this.metadata = metadata;
        this.executor = executor;
        this.operationFactory = new OperationFactory();
        this.truncationMarkers = new TruncationMarkerCollection();
        this.inMemoryOperationLog = new MemoryOperationLog();
        this.memoryLogUpdater = new MemoryLogUpdater(this.inMemoryOperationLog, cache);
        this.operationProcessor = new OperationProcessor(this.metadata.getContainerId(), new OperationMetadataUpdater(this.metadata, this.truncationMarkers), this.memoryLogUpdater, this.dataFrameLog);
        this.operationProcessor.addListener(new ServiceShutdownListener(this::queueStoppedHandler, this::queueFailedHandler), this.executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            this.operationProcessor.close();
            this.dataFrameLog.close();
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStart");

        this.executor.execute(() -> {
            try {
                performRecovery();
                this.operationProcessor.startAsync().awaitRunning();
            } catch (Exception ex) {
                if (this.operationProcessor.isRunning()) {
                    // Make sure we stop the operation processor if we started it.
                    this.operationProcessor.stopAsync();
                }

                notifyFailed(ex);
                return;
            }

            // If we got here, all is good. We were able to start successfully.
            notifyStarted();
            LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
        });
    }

    @Override
    protected void doStop() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStop");
        this.operationProcessor.stopAsync();

        this.executor.execute(() -> {
            ServiceShutdownListener.awaitShutdown(this.operationProcessor, false);

            Throwable cause = this.stopException.get();
            if (cause == null && this.operationProcessor.state() == State.FAILED) {
                cause = this.operationProcessor.failureCause();
            }

            if (cause == null) {
                // Normal shutdown.
                notifyStopped();
            } else {
                // Shutdown caused by some failure.
                notifyFailed(cause);
            }

            LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
        });
    }

    //endregion

    //region Container Implementation

    @Override
    public String getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region OperationLog Implementation

    @Override
    public CompletableFuture<Long> add(Operation operation, Duration timeout) {
        ensureRunning();
        return this.operationProcessor.process(operation);
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequenceNumber, Duration timeout) {
        ensureRunning();
        long dataFrameSeqNo = this.truncationMarkers.getClosestTruncationMarker(upToSequenceNumber);
        if (dataFrameSeqNo < 0) {
            // Nothing to truncate.
            return CompletableFuture.completedFuture(null);
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        log.info("{}: Truncate (OperationSequenceNumber = {}, DataFrameSequenceNumber = {}).", this.traceObjectId, upToSequenceNumber, dataFrameSeqNo);

        return snapshotMetadata()
                .thenCompose(v -> this.dataFrameLog.truncate(dataFrameSeqNo, timer.getRemaining())) // Truncate DataFrameLog.
                .thenRun(() -> {
                    // Truncate InMemory Transaction Log.
                    this.inMemoryOperationLog.truncate(e -> e.getSequenceNumber() <= upToSequenceNumber);

                    // Remove old truncation markers.
                    this.truncationMarkers.removeTruncationMarkers(upToSequenceNumber);
                });
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        ensureRunning();
        log.debug("{}: Read (AfterSequenceNumber = {}, MaxCount = {}).", this.traceObjectId, afterSequenceNumber, maxCount);
        // TODO: have this Future block if we are trying to read at the very end of the log. We will need this once
        // we implement the LogSynchronizer.
        return CompletableFuture.completedFuture(this.inMemoryOperationLog.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount));
    }

    //endregion

    //region Recovery

    private void performRecovery() throws Exception {
        // Make sure we are in the correct state. We do not want to do recovery while we are in full swing.
        Preconditions.checkState(state() == State.STARTING, "Cannot perform recovery if the DurableLog is not in a '%s' state.", State.STARTING);

        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "performRecovery");
        TimeoutTimer timer = new TimeoutTimer(RECOVERY_TIMEOUT);
        log.info("{} Recovery started.", this.traceObjectId);

        // Put metadata (and entire container) into 'Recovery Mode'.
        this.metadata.enterRecoveryMode();
        this.truncationMarkers.enterRecoveryMode();

        // Reset metadata.
        this.metadata.reset();
        this.truncationMarkers.reset();

        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(this.metadata, this.truncationMarkers);
        this.memoryLogUpdater.enterRecoveryMode(metadataUpdater);

        boolean success = false;
        try {
            this.dataFrameLog.initialize(timer.getRemaining());
            recoverFromDataFrameLog(metadataUpdater);
            log.info("{} Recovery completed.", this.traceObjectId);
            success = true;
        } catch (Exception ex) {
            log.error("{} Recovery FAILED. {}", this.traceObjectId, ex);
            throw ex;
        } finally {
            // We must exit recovery mode when done, regardless of outcome.
            this.metadata.exitRecoveryMode();
            this.truncationMarkers.exitRecoveryMode();
            this.memoryLogUpdater.exitRecoveryMode(this.metadata, success);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "performRecovery", traceId);
    }

    private void recoverFromDataFrameLog(OperationMetadataUpdater metadataUpdater) throws Exception {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "recoverFromDataFrameLog");

        // Read all entries from the DataFrameLog and append them to the InMemoryOperationLog.
        // Also update metadata along the way.
        try (DataFrameReader<Operation> reader = new DataFrameReader<>(this.dataFrameLog, this.operationFactory, getId())) {
            DataFrameReader.ReadResult lastReadResult = null;
            while (true) {
                // Fetch the next operation.
                DataFrameReader.ReadResult<Operation> readResult = reader.getNext();
                if (readResult == null) {
                    // We have reached the end.
                    break;
                }

                Operation operation = readResult.getItem();

                // Update Metadata Sequence Number.
                this.metadata.setOperationSequenceNumber(operation.getSequenceNumber());

                // Determine Truncation Markers.
                if (readResult.isLastFrameEntry()) {
                    // The current Log Operation was the last one in the frame. This is a Truncation Marker.
                    metadataUpdater.recordTruncationMarker(operation.getSequenceNumber(), readResult.getDataFrameSequence());
                } else if (lastReadResult != null && !lastReadResult.isLastFrameEntry() && readResult.getDataFrameSequence() != lastReadResult.getDataFrameSequence()) {
                    // DataFrameSequence changed on this operation (and this operation spans multiple frames). The Truncation Marker is on this operation, but the previous frame.
                    metadataUpdater.recordTruncationMarker(operation.getSequenceNumber(), lastReadResult.getDataFrameSequence());
                }

                lastReadResult = readResult;

                // Process the operation.
                try {
                    log.debug("{} Recovering {}.", this.traceObjectId, operation);
                    if (operation instanceof MetadataOperation) {
                        metadataUpdater.processMetadataOperation((MetadataOperation) operation);
                    } else if (operation instanceof StorageOperation) {
                        //TODO: should we also check that streams still exist in Storage, and that their lengths are what we think they are? Or we leave that to the LogSynchronizer?
                        metadataUpdater.preProcessOperation((StorageOperation) operation);
                        metadataUpdater.acceptOperation((StorageOperation) operation);
                    }
                } catch (StreamSegmentException | MetadataUpdateException ex) {
                    // Metadata updates failures should not happen during recovery.
                    throw new DataCorruptionException(String.format("Unable to update metadata for Log Operation %s", operation), ex);
                }

                // Add to InMemory Operation Log.
                this.memoryLogUpdater.add(operation);
            }
        }

        // Commit whatever changes we have in the metadata updater to the Container Metadata.
        // This code will only be invoked if we haven't encountered any exceptions during recovery.
        metadataUpdater.commit();
        LoggerHelpers.traceLeave(log, this.traceObjectId, "recoverFromDataFrameLog", traceId);
    }

    //endregion

    //region Helpers

    private CompletableFuture<Void> snapshotMetadata() {
        // TODO: Implement properly. And include in Unit Tests.
        return CompletableFuture.completedFuture(null);
    }

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed, this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void queueFailedHandler(Throwable cause) {
        // The Queue Processor failed. We need to shut down right away.
        log.warn("{}: QueueProcessor failed with exception {}", this.traceObjectId, cause);
        this.stopException.set(cause);
        stopAsync();
    }

    private void queueStoppedHandler() {
        if (state() != State.STOPPING && state() != State.FAILED) {
            // The Queue Processor stopped but we are not in a stopping phase. We need to shut down right away.
            log.warn("{}: QueueProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping. Shutting down DurableLog.", this.traceObjectId);
            this.stopException.set(new StreamingException("QueueProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping."));
            stopAsync();
        }
    }

    //endregion
}
