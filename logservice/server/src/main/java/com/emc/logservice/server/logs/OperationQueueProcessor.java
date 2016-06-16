package com.emc.logservice.server.logs;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.common.LoggerHelpers;
import com.emc.logservice.server.*;
import com.emc.logservice.server.logs.operations.*;
import com.emc.logservice.storageabstraction.DurableDataLog;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Single-thread Processor for OperationQueue. Takes all entries currently available from the OperationQueue,
 * generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
public class OperationQueueProcessor implements Container {
    //region Members

    private static final Duration CloseTimeout = Duration.ofSeconds(30);
    private final String traceObjectId;
    private final String containerId;
    private final OperationQueue operationQueue;
    private final OperationMetadataUpdater metadataUpdater;
    private final MemoryLogUpdater logUpdater;
    private final DurableDataLog durableDataLog;
    private final FaultHandlerRegistry faultRegistry;
    private final Object StateTransitionLock = new Object(); // TODO: consider using AsyncLock
    private Thread runThread;
    private ContainerState state;
    private CompletableFuture<Void> stopFuture;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationQueueProcessor class.
     *
     * @param containerId     The Id of the container this QueueProcessor belongs to.
     * @param operationQueue  The Operation Queue to work on.
     * @param metadataUpdater An OperationMetadataUpdater to work with.
     * @param logUpdater      A MemoryLogUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog  The DataFrameLog to write DataFrames to.
     * @throws NullPointerException If any of the arguments are null.
     */
    public OperationQueueProcessor(String containerId, OperationQueue operationQueue, OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, DurableDataLog durableDataLog) {
        Exceptions.throwIfNull(containerId, "containerId");
        Exceptions.throwIfNull(operationQueue, "operationQueue");
        Exceptions.throwIfNull(metadataUpdater, "metadataUpdater");
        Exceptions.throwIfNull(logUpdater, "logUpdater");
        Exceptions.throwIfNull(durableDataLog, "durableDataLog");

        this.traceObjectId = String.format("OperationQueueProcessor[%s]", containerId);
        this.containerId = containerId;
        this.operationQueue = operationQueue;
        this.metadataUpdater = metadataUpdater;
        this.logUpdater = logUpdater;
        this.durableDataLog = durableDataLog;
        this.faultRegistry = new FaultHandlerRegistry();
        setState(ContainerState.Created);
    }

    //endregion

    //region AutoCloseable implementation

    @Override
    public void close() {
        stop(CloseTimeout).join();
        setState(ContainerState.Closed);
    }

    //endregion

    //region Container Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "initialize");
        synchronized (StateTransitionLock) {
            ContainerState.Initialized.checkValidPreviousState(this.state);

            // No real initialization work needed.
            setState(ContainerState.Initialized);
        }

        LoggerHelpers.traceLeave(log, traceObjectId, "initialize", traceId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> start(Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "start");
        Thread runThread;
        synchronized (StateTransitionLock) {
            ContainerState.Started.checkValidPreviousState(this.state);
            Exceptions.throwIfIllegalState(this.runThread == null, "OperationQueueProcessor is already running.");

            this.runThread = runThread = new Thread(this::runContinuously);
            setState(ContainerState.Started);
        }

        runThread.start();
        LoggerHelpers.traceLeave(log, traceObjectId, "start", traceId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stop(Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "stop");
        CompletableFuture<Void> result = null;
        synchronized (StateTransitionLock) {
            if (getState() != ContainerState.Started) {
                // Only bother to stop if we are actually running.
                return CompletableFuture.completedFuture(null);
            }

            if (this.runThread != null) {
                this.stopFuture = result = new CompletableFuture<>();
                this.runThread.interrupt();
            }
        }

        if (result == null) {
            result = CompletableFuture.completedFuture(null);
        }

        return result.thenRun(() -> LoggerHelpers.traceLeave(log, traceObjectId, "stop", traceId));
    }

    @Override
    public void registerFaultHandler(Consumer<Throwable> handler) {
        this.faultRegistry.register(handler);
    }

    @Override
    public ContainerState getState() {
        return this.state;
    }

    @Override
    public String getId() {
        return this.containerId;
    }

    //endregion

    //region Queue Processing

    /**
     * Main thread body. Runs in a continuous loop until interrupted.
     */
    private void runContinuously() {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "runContinuously");
        try {
            while (true) {
                try {
                    runOnce();
                }
                catch (InterruptedException ex) {
                    // We are done. Exit.
                    break;
                }
                catch (Exception ex) {
                    // Catch-all for exceptions. If any exception bubbled up to here, we need to stop processing and
                    // report the exception up.
                    this.faultRegistry.handle(ex);
                    break;
                }
            }
        }
        finally {
            // Always run cleanup.
            postRun();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "runContinuously", traceId);
        }
    }

    /**
     * Single iteration of the Queue Processor.
     * Steps:
     * <ol>
     * <li> Picks all items from the Queue. If none, exits.
     * <li> Creates a DataFrameBuilder and starts appending items to it.
     * <li> As the DataFrameBuilder acknowledges DataFrames being published, acknowledge the corresponding Operations as well.
     * </ol>
     *
     * @throws InterruptedException
     */
    private void runOnce() throws InterruptedException {
        Queue<CompletableOperation> operations = this.operationQueue.takeAllEntries();
        log.debug("{}: RunOnce (OperationCount = {}).", this.traceObjectId, operations.size());
        if (operations.size() == 0) {
            // takeAllEntries() should have been blocking and not return unless it has data. If we get an empty response, just try again.
            return;
        }

        QueueProcessingState state = new QueueProcessingState(this.metadataUpdater, this.logUpdater, this.faultRegistry::handle, this.traceObjectId);
        try (DataFrameBuilder dataFrameBuilder = new DataFrameBuilder(this.durableDataLog, state::commit, state::fail)) {
            for (CompletableOperation o : operations) {
                boolean processedSuccessfully = processOperation(o, dataFrameBuilder);

                // Add the operation as 'pending', only if we were able to successfully append it to a data frame.
                // We only commit data frames when we attempt to start a new record (if it's full) or if we try to close it, so we will not miss out on it.
                if (processedSuccessfully) {
                    state.addPending(o);
                }
            }
        }
        // Close the frame builder and ship any unsent frames.
        catch (SerializationException ex) {
            state.fail(ex);
        }
    }

    /**
     * Processes a single operation.
     * Steps:
     * <ol>
     * <li> Pre-processes operation (in MetadataUpdater)
     * <li> Assigns Sequence Number
     * <li> Appends to DataFrameBuilder
     * <li> Accepts operation in MetadataUpdater.
     * </ol>
     * Any exceptions along the way will result in the immediate failure of the operations. Exceptions do not bubble out
     * of this method. The only way to determine whether the operation completed normally or not is to inspect the result.
     *
     * @param operation        The operation to process.
     * @param dataFrameBuilder The DataFrameBuilder to append the operation to.
     * @return True if processed successfully, false otherwise.
     */
    private boolean processOperation(CompletableOperation operation, DataFrameBuilder dataFrameBuilder) {
        Exceptions.throwIfIllegalState(!operation.isDone(), "The Operation has already been processed.");

        // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater has all the knowledge for that task.
        Operation entry = operation.getOperation();
        if (entry instanceof StorageOperation) {
            // We only need to update metadata for StorageOperations; MetadataOperations are internal and are processed by the requester.
            // We do this in two steps: first is pre-processing (updating the entry with offsets, lengths, etc.) and the second is acceptance.
            // Pre-processing does not have any effect on the metadata, but acceptance does.
            // That's why acceptance has to happen only after a successful append to the DataFrameBuilder.
            try {
                this.metadataUpdater.preProcessOperation((StorageOperation) entry);
            }
            catch (Exception ex) {
                // This entry was not accepted (due to external error) or some processing error occurred. Our only option is to fail it now, before trying to commit it.
                operation.fail(ex);
                return false;
            }
        }

        // Entry is ready to be serialized; assign a sequence number.
        entry.setSequenceNumber(this.metadataUpdater.getNewOperationSequenceNumber());

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
        try {
            dataFrameBuilder.append(operation.getOperation());
        }
        catch (Exception ex) {
            operation.fail(ex);
            return false;
        }

        if (entry instanceof StorageOperation) {
            try {
                this.metadataUpdater.acceptOperation((StorageOperation) entry);
            }
            catch (MetadataUpdateException ex) {
                // This is an internal error. This shouldn't happen. The entry has been committed, but we couldn't update the metadata due to a bug.
                operation.fail(ex);
                return false;
            }
        }

        return true;
    }

    /**
     * Post-run cleanup.
     */
    private void postRun() {
        // Exiting: need to cleanup
        CompletableFuture<Void> resultFuture;
        synchronized (StateTransitionLock) {
            // Make sure we set the container state to Stopped once we exit the main loop.
            this.runThread = null;
            resultFuture = this.stopFuture;
            this.stopFuture = null;
            setState(ContainerState.Stopped);
        }

        if (resultFuture != null) {
            resultFuture.complete(null);
        }
    }

    private void setState(ContainerState state) {
        if (state != this.state) {
            log.info("{}: StateTransition from {} to {}.", traceObjectId, this.state, state);
            this.state = state;
        }
    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the QueueProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     */
    @Slf4j
    private static class QueueProcessingState {
        private final String traceObjectId;
        private final LinkedList<CompletableOperation> pendingOperations;
        private final OperationMetadataUpdater metadataUpdater;
        private final MemoryLogUpdater logUpdater;
        private final Consumer<Exception> criticalErrorHandler;

        public QueueProcessingState(OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, Consumer<Exception> criticalErrorHandler, String traceObjectId) {
            assert metadataUpdater != null : "metadataUpdater is null";
            assert logUpdater != null : "logUpdater is null";
            assert criticalErrorHandler != null :"criticalErrorHandler is null";

            this.traceObjectId = traceObjectId;
            this.pendingOperations = new LinkedList<>();
            this.metadataUpdater = metadataUpdater;
            this.logUpdater = logUpdater;
            this.criticalErrorHandler = criticalErrorHandler;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        public void addPending(CompletableOperation operation) {
            this.pendingOperations.add(operation);
        }

        /**
         * Commits all pending Metadata changes, assigns a TruncationMarker and acknowledges all the pending operations.
         *
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        public void commit(DataFrameBuilder.DataFrameCommitArgs commitArgs) {
            log.debug("{}: CommitSuccess (OperationCount = {}).", this.traceObjectId, this.pendingOperations.size());

            // Commit any changes to metadata.
            this.metadataUpdater.commit();
            this.metadataUpdater.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getDataFrameSequence());

            // TODO: consider running this on its own thread, but they must still be in the same sequence!
            // Acknowledge all pending entries, in the order in which they are in the queue. It is important that we ack entries in order of increasing Sequence Number.
            while (this.pendingOperations.size() > 0 && this.pendingOperations.getFirst().getOperation().getSequenceNumber() <= commitArgs.getLastFullySerializedSequenceNumber()) {
                CompletableOperation e = this.pendingOperations.removeFirst();
                try {
                    logUpdater.add(e.getOperation());
                }
                catch (DataCorruptionException ex) {
                    log.error("{}: OperationCommitFailure ({}). {}", this.traceObjectId, e.getOperation(), ex);
                    this.criticalErrorHandler.accept(ex);
                    e.fail(ex);
                    return;
                }

                e.complete();
            }

            this.logUpdater.flush();
        }

        /**
         * Rolls back all pending Metadata changes and fails all pending operations.
         *
         * @param ex The cause of the failure. The operations will be failed with this as a cause.
         */
        public void fail(Throwable ex) {
            log.error("{}: CommitFailure ({} operations). {}", this.traceObjectId, this.pendingOperations.size(), ex);

            // Discard all updates to the metadata.
            this.metadataUpdater.rollback();

            // Fail all pending entries.
            this.pendingOperations.forEach(e -> e.fail(ex));
            this.pendingOperations.clear();
        }
    }

    //endregion
}
