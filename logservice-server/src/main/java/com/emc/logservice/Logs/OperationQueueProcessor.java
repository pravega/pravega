package com.emc.logservice.Logs;

import com.emc.logservice.*;
import com.emc.logservice.Logs.Operations.CompletableOperation;
import com.emc.logservice.Logs.Operations.Operation;
import com.emc.logservice.Logs.Operations.StorageOperation;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Single-thread Processor for OperationQueue. Takes all entries currently available from the OperationQueue,
 * generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
public class OperationQueueProcessor implements Container
{
    //region Members

    private static final Duration AutoCloseTimeout = Duration.ofSeconds(30);
    private static final int MaxDataFrameSize = 1024 * 1024; // 1MB
    private final OperationQueue operationQueue;
    private final OperationMetadataUpdater metadataUpdater;
    private final MemoryLogUpdater logUpdater;
    private final DataFrameLog dataFrameLog;
    private final Object StateTransitionLock = new Object(); // TODO: consider using AsyncLock
    private Thread runThread;
    private ContainerState state;
    private CompletableFuture<Void> stopFuture;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationQueueProcessor class.
     *
     * @param operationQueue  The Operation Queue to work on.
     * @param metadataUpdater An OperationMetadataUpdater to work with.
     * @param logUpdater      A MemoryLogUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param dataFrameLog    The DataFrameLog to write DataFrames to.
     * @throws NullPointerException If any of the arguments are null.
     */
    public OperationQueueProcessor(OperationQueue operationQueue, OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, DataFrameLog dataFrameLog)
    {
        if (operationQueue == null)
        {
            throw new NullPointerException("operationQueue");
        }

        if (metadataUpdater == null)
        {
            throw new NullPointerException("metadataUpdater");
        }

        if (logUpdater == null)
        {
            throw new NullPointerException("logUpdater");
        }

        if (dataFrameLog == null)
        {
            throw new NullPointerException("dataFrameLog");
        }

        this.operationQueue = operationQueue;
        this.metadataUpdater = metadataUpdater;
        this.logUpdater = logUpdater;
        this.dataFrameLog = dataFrameLog;
        setState(ContainerState.Created);
    }

    //endregion

    //region AutoCloseable implementation

    @Override
    public void close() throws Exception
    {
        stop(AutoCloseTimeout).get(AutoCloseTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    //endregion

    //region Container Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout)
    {
        synchronized (StateTransitionLock)
        {
            ContainerState.Initialized.checkValidPreviousState(this.state);

            // TODO: do any initialization work.
            setState(ContainerState.Initialized);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> start(Duration timeout)
    {
        Thread runThread;
        synchronized (StateTransitionLock)
        {
            ContainerState.Started.checkValidPreviousState(this.state);

            if (this.runThread != null)
            {
                throw new IllegalStateException("Internal error: OperationQueueProcessor was already running ");
            }

            this.runThread = runThread = new Thread(this::runContinuously);
            setState(ContainerState.Started);
        }

        runThread.start();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stop(Duration timeout)
    {
        CompletableFuture<Void> result = null;
        synchronized (StateTransitionLock)
        {
            if (getState() != ContainerState.Started)
            {
                // Only bother to stop if we are actually running.
                return CompletableFuture.completedFuture(null);
            }

            if (this.runThread != null)
            {
                this.stopFuture = result = new CompletableFuture<>();
                this.runThread.interrupt();
            }
        }

        if (result == null)
        {
            result = CompletableFuture.completedFuture(null);
        }

        return result;
    }

    @Override
    public void registerFaultHandler(Consumer<Throwable> handler)
    {
        //TODO: implement
    }

    @Override
    public ContainerState getState()
    {
        return this.state;
    }

    //endregion

    //region Queue Processing

    /**
     * Main thread body. Runs in a continuous loop until interrupted.
     */
    private void runContinuously()
    {
        try
        {
            while (true)
            {
                try
                {
                    runOnce();
                }
                catch (InterruptedException ex)
                {
                    break;
                }
//                catch (Exception ex)
//                {
//                    //TODO: TBD
//                    System.out.println(ex);
//                }
            }
        }
        finally
        {
            // Always run cleanup.
            postRun();
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
    private void runOnce() throws InterruptedException
    {
        Queue<CompletableOperation> operations = this.operationQueue.takeAllEntries();
        if (operations.size() == 0)
        {
            // takeAllEntries() should have been blocking and not return unless it has data. If we get an empty response, just try again.
            return;
        }

        DataFrameBuildState state = new DataFrameBuildState(this.metadataUpdater, this.logUpdater, this::handleCriticalError);
        try (DataFrameBuilder dataFrameBuilder = new DataFrameBuilder(MaxDataFrameSize, this.dataFrameLog, state::commit, state::fail))
        {
            for (CompletableOperation o : operations)
            {
                boolean processedSuccessfully = processOperation(o, dataFrameBuilder);

                // Add the operation as 'pending', only if we were able to successfully add it to a data frame.
                // We only commit data frames when we attempt to start a new record (if it's full) or if we try to close it, so we will not miss out on it.
                if (processedSuccessfully)
                {
                    state.addPending(o);
                }
            }
        }
        // Close the frame builder and ship any unsent frames.
        catch (SerializationException ex)
        {
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
     * @param dataFrameBuilder The DataFrameBuilder to add the operation to.
     * @return True if processed successfully, false otherwise.
     */
    private boolean processOperation(CompletableOperation operation, DataFrameBuilder dataFrameBuilder)
    {
        if (operation.isDone())
        {
            throw new IllegalStateException("Entry has already been processed.");
        }

        // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater has all the knowledge for that task.
        Operation entry = operation.getOperation();
        if (entry instanceof StorageOperation)
        {
            // We only need to update metadata for StorageOperations; MetadataOperations are internal and are processed by the requester.
            // We do this in two steps: first is pre-processing (updating the entry with offsets, lengths, etc.) and the second is acceptance.
            // Pre-processing does not have any effect on the metadata, but acceptance does.
            // That's why acceptance has to happen only after a successful add to the DataFrameBuilder.
            try
            {
                this.metadataUpdater.preProcessOperation((StorageOperation) entry);
            }
            catch (Exception ex)
            {
                // This entry was not accepted (due to external error) or some processing error occurred. Our only option is to fail it now, before trying to commit it.
                operation.fail(ex);
                return false;
            }
        }

        // Entry is ready to be serialized; assign a sequence number.
        entry.setSequenceNumber(this.metadataUpdater.getNewOperationSequenceNumber());

        try
        {
            dataFrameBuilder.append(operation.getOperation());
        }
        catch (Exception ex)
        {
            operation.fail(ex);
            return false;
        }

        if (entry instanceof StorageOperation)
        {
            try
            {
                this.metadataUpdater.acceptOperation((StorageOperation) entry);
            }
            catch (MetadataUpdateException ex)
            {
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
    private void postRun()
    {
        // Exiting: need to cleanup
        CompletableFuture<Void> resultFuture;
        synchronized (StateTransitionLock)
        {
            // Make sure we set the container state to Stopped once we exit the main loop.
            this.runThread = null;
            resultFuture = this.stopFuture;
            this.stopFuture = null;
            setState(ContainerState.Stopped);
        }

        if (resultFuture != null)
        {
            resultFuture.complete(null);
        }
    }

    private void setState(ContainerState state)
    {
        this.state = state;
    }

    //endregion

    //region Helpers

    private void handleCriticalError(Exception ex)
    {
        //TODO: better...
        System.err.println(ex);
    }

    //endregion

    //region DataFrameBuildState

    /**
     * Temporary State for DataFrameBuilder. Keeps track of pending Operations and allows committing or failing all of them.
     */
    private class DataFrameBuildState
    {
        private final LinkedList<CompletableOperation> pendingOperations;
        private final OperationMetadataUpdater metadataUpdater;
        private final MemoryLogUpdater logUpdater;
        private final Consumer<Exception> criticalErrorHandler;

        public DataFrameBuildState(OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, Consumer<Exception> criticalErrorHandler)
        {
            this.pendingOperations = new LinkedList<>();
            this.metadataUpdater = metadataUpdater;
            this.logUpdater = logUpdater;
            this.criticalErrorHandler = criticalErrorHandler;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to add.
         */
        public void addPending(CompletableOperation operation)
        {
            this.pendingOperations.add(operation);
        }

        /**
         * Commits all pending Metadata changes, assigns a TruncationMarker and acknowledges all the pending operations.
         *
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        public void commit(DataFrameBuilder.DataFrameCommitArgs commitArgs)
        {
            // Commit any changes to metadata.
            this.metadataUpdater.commit();
            this.metadataUpdater.commitTruncationMarker(new TruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getDataFrameSequence()));

            // TODO: consider running this on its own thread, but they must still be in the same sequence!
            // Acknowledge all pending entries, in the order in which they are in the queue. It is important that we ack entries in order of increasing Sequence Number.
            while (this.pendingOperations.size() > 0 && this.pendingOperations.getFirst().getOperation().getSequenceNumber() <= commitArgs.getLastFullySerializedSequenceNumber())
            {
                CompletableOperation e = this.pendingOperations.removeFirst();
                try
                {
                    logUpdater.add(e.getOperation());
                }
                catch (DataCorruptionException ex)
                {
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
        public void fail(Exception ex)
        {
            // Discard all updates to the metadata.
            this.metadataUpdater.rollback();

            // Fail all pending entries.
            this.pendingOperations.forEach(e -> e.fail(ex));
            this.pendingOperations.clear();
        }
    }

    //endregion
}
