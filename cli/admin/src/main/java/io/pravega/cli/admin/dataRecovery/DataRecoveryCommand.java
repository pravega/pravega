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
package io.pravega.cli.admin.dataRecovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.DebugBookKeeperLogWrapper;
import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Base class for data recovery related commands' classes.
 */
public abstract class DataRecoveryCommand extends AdminCommand {
    protected final static String COMPONENT = "data-recovery";
    final static Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates the {@link StorageFactory} instance by reading the config values.
     *
     * @param executorService   A thread pool for execution.
     * @return                  A newly created {@link StorageFactory} instance.
     */
    StorageFactory createStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getCommandArgs().getState().getConfigBuilder().build());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    int validateBackupLog(DurableDataLogFactory dataLogFactory, int containerId, DebugDurableDataLogWrapper originalDataLog,
                                  boolean createNewBackupLog) throws Exception {
        // Validate that the Original and Backup logs have the same number of operations.
        int operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback((a, b) -> { }, containerId, originalDataLog.asReadOnly());
        @Cleanup
        val validationBackupDataLog = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId());
        @Cleanup
        val validationBackupDataLogReadOnly = validationBackupDataLog.asReadOnly();
        int backupLogReadOperations = readDurableDataLogWithCustomCallback((a, b) ->
                output("Reading: " + a), dataLogFactory.getBackupLogId(), validationBackupDataLogReadOnly);
        output("Original DurableLog operations read: " + operationsReadFromOriginalLog +
                ", Backup DurableLog operations read: " + backupLogReadOperations);

        // Ensure that the Original log contains the same number of Operations than the Backup log upon a new log read.
        Preconditions.checkState(!createNewBackupLog || operationsReadFromOriginalLog == backupLogReadOperations,
                "Operations read from Backup Log (" + backupLogReadOperations + ") differ from Original Log ones (" + operationsReadFromOriginalLog + ") ");
        return backupLogReadOperations;
    }

    /**
     * Returns whether it exists a {@link DurableDataLog} with the reserved id for Backup Log (BACKUP_LOG_ID).
     *
     * @param dataLogFactory Factory to instantiate {@link DurableDataLog} instances.
     * @return Whether there is metadata for an existing Backup Log.
     * @throws DataLogInitializationException If there is an error initializing the {@link DurableDataLog}.
     */
    boolean existsBackupLog(DurableDataLogFactory dataLogFactory) throws Exception {
        try (DebugDurableDataLogWrapper backupDataLogDebugLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId())) {
            return backupDataLogDebugLogWrapper.fetchMetadata() != null;
        }
    }

    /**
     * Copies the contents of the input containerId and {@link DebugBookKeeperLogWrapper} on a new log with id BACKUP_LOG_ID.
     *
     * @param dataLogFactory Factory to instantiate {@link DurableDataLog} instances.
     * @param containerId Container id for the source log.
     * @param originalDataLog Source log.
     * @throws Exception If there is an error during backing up process.
     */
    void createBackupLog(DurableDataLogFactory dataLogFactory, int containerId, DebugDurableDataLogWrapper originalDataLog) throws Exception {
        // Create a new Backup Log to store the Original Log contents.
        @Cleanup
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(dataLogFactory.getBackupLogId());
        backupDataLog.initialize(TIMEOUT);

        // Instantiate the processor that will back up the Original Log contents into the Backup Log.
        int operationsReadFromOriginalLog;
        try (BackupLogProcessor backupLogProcessor = new BackupLogProcessor(backupDataLog, getCommandArgs().getState().getExecutor());
             DurableDataLog originalDataLogReadOnly = originalDataLog.asReadOnly()) {
            operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback(backupLogProcessor, containerId, originalDataLogReadOnly);
            // The number of processed operation should match the number of read operations from DebugRecoveryProcessor.
            checkBackupLogAssertions(backupLogProcessor.getBeforeCommit().get(), backupLogProcessor.getCommitSuccess().get(),
                    operationsReadFromOriginalLog, backupLogProcessor.isFailed);
        } catch (Exception e) {
            outputError("There have been errors while creating the Backup Log.");
            throw e;
        }
    }


    /**
     * Reads a {@link DurableDataLog} associated with a container id and runs the callback on each {@link Operation}
     * read from the log.
     *
     * @param callback Callback to be run upon each {@link Operation} read.
     * @param containerId Container id to read from.
     * @param durableDataLog {@link DurableDataLog} of the Container to be read.
     * @return Number of {@link Operation}s read.
     * @throws Exception If there is a problem reading the {@link DurableDataLog}.
     */
    @VisibleForTesting
    int readDurableDataLogWithCustomCallback(BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> callback,
                                             int containerId, DurableDataLog durableDataLog) throws Exception {
        val logReaderCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                callback,
                op -> false, // We are not interested on doing actual recovery, just reading the operations.
                null,
                null);
        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);
        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, durableDataLog,
                containerConfig, readIndexConfig, getCommandArgs().getState().getExecutor(), logReaderCallbacks, false);
        int operationsRead = rp.performRecovery();
        output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
    }

    /**
     * Performs some basic correctness checks on a backup log.
     *
     * @param beforeCommitCalls Number of executions of beforeCommit callbacks during the processing.
     * @param commitSuccessCalls Number of executions of commitSuccess callbacks during the processing.
     * @param operationsReadFromOriginalLog Number of {@link Operation}s read from the Original Log.
     * @param isFailed Whether the {@link BackupLogProcessor} has found any failure during processing.
     */
    @VisibleForTesting
    void checkBackupLogAssertions(long beforeCommitCalls, long commitSuccessCalls, long operationsReadFromOriginalLog, boolean isFailed) {
        Preconditions.checkState(beforeCommitCalls == commitSuccessCalls, "BackupLogProcessor has different number of processed (" + beforeCommitCalls +
                ") and successful operations (" + commitSuccessCalls + ")");
        Preconditions.checkState(commitSuccessCalls == operationsReadFromOriginalLog, "BackupLogProcessor successful operations (" + commitSuccessCalls +
                ") differs from Original Log operations (" + operationsReadFromOriginalLog + ")");
        Preconditions.checkState(!isFailed, "BackupLogProcessor has failed");
    }

    /**
     * This class provides the basic logic for reading from a {@link DurableDataLog} and writing the read {@link Operation}s
     * to another {@link DurableDataLog}. Internally, it uses a {@link DataFrameBuilder} to write to the target {@link DurableDataLog}
     * and performs one write at a time, waiting for the previous write to complete before issuing the next one. It also
     * provides counters to inform about the state of the processing as well as closing the resources.
     */
    abstract class AbstractLogProcessor implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>>, AutoCloseable {

        protected final Map<Long, CompletableFuture<Void>> operationProcessingTracker = new ConcurrentHashMap<>();
        @NonNull
        protected final DataFrameBuilder<Operation> dataFrameBuilder;
        @Getter
        protected boolean isFailed = false;
        @Getter
        private final AtomicBoolean closed = new AtomicBoolean();
        @Getter
        private final AtomicInteger beforeCommit = new AtomicInteger();
        @Getter
        private final AtomicInteger commitSuccess = new AtomicInteger();
        @Getter
        private final AtomicInteger commitFailure = new AtomicInteger();
        private final AtomicLong sequenceNumber = new AtomicLong(Long.MIN_VALUE);

        AbstractLogProcessor(DurableDataLog durableDataLog, ScheduledExecutorService executor) {
            DataFrameBuilder.Args args = new DataFrameBuilder.Args(
                    a -> this.beforeCommit.getAndIncrement(),
                    b -> {
                        this.operationProcessingTracker.get(b.getLastFullySerializedSequenceNumber()).complete(null);
                        this.commitSuccess.getAndIncrement();
                    },
                    (c, d) -> {
                        this.operationProcessingTracker.get(d.getLastFullySerializedSequenceNumber()).complete(null);
                        this.isFailed = true; // Consider a single failed write as a failure in the whole process.
                        this.commitFailure.getAndIncrement();
                    },
                    executor);
            this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                this.dataFrameBuilder.flush();
                this.dataFrameBuilder.close();
                this.operationProcessingTracker.clear();
            }
        }

        /**
         * Writes an {@link Operation} to the {@link DataFrameBuilder} and wait for the {@link DataFrameBuilder.Args}
         * callbacks are invoked after the operation is written to the target {@link DurableDataLog}.
         *
         * @param operation {@link Operation} to be written and completed before continue with further writes.
         * @throws IOException If there is a problem writing the {@link Operation} to the target {@link DurableDataLog}.
         */
        protected void writeAndConfirm(Operation operation) throws IOException {
            sequenceNumber.compareAndSet(Long.MIN_VALUE, operation.getSequenceNumber());
            // We only consider writing operations with sequence number higher than the expected one.
            if (operation.getSequenceNumber() >= sequenceNumber.get()) {
                trackOperation(operation);
                this.dataFrameBuilder.append(operation);
                this.dataFrameBuilder.flush();
                waitForOperationCommit(operation);
                sequenceNumber.incrementAndGet();
            } else {
                outputError("Skipping (i.e., not writing) Operation with wrong Sequence Number: " + operation);
            }
        }

        private void trackOperation(Operation operation) {
            this.operationProcessingTracker.put(operation.getSequenceNumber(), new CompletableFuture<>());
        }

        private void waitForOperationCommit(Operation operation) {
            this.operationProcessingTracker.get(operation.getSequenceNumber()).join();
            this.operationProcessingTracker.remove(operation.getSequenceNumber());
        }
    }

    /**
     * Writes all the {@link Operation}s passed in the callback to the target {@link DurableDataLog}.
     */
    class BackupLogProcessor extends AbstractLogProcessor {

        BackupLogProcessor(DurableDataLog backupDataLog, ScheduledExecutorService executor) {
            super(backupDataLog, executor);
        }

        @Override
        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                output("Backing up: " + operation);
                writeAndConfirm(operation);
            } catch (Exception e) {
                outputError("Error writing Operation to Backup Log " + operation);
                outputException(e);
                isFailed = true;
            }
        }
    }


    static class AbortedUserOperation extends RuntimeException {
    }
}
