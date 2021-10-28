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
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * This command provides an administrator with the basic primitives to manipulate a DurableLog with damaged entries.
 * The workflow of this command is as follows:
 * 1. Disable the original DurableLog (if not done yet).
 * 2. Reads the original damaged DurableLog and creates a backup copy of it for safety reasons.
 * 3. Validate and buffer all the edits from admin to be done on the original DurableLog data (i.e., skip, delete,
 * replace operations). All these changes are then written on a Repair Log (i.e., original DurableLog data + admin changes).
 * 4. With the desired updates written in the Repair Log, the admin can replace the original DurableLog metadata by the
 * Repair Log's one. This will make the DurableLog for the Segment Container under repair to point to the Repair Log data.
 * 5. The backed-up data for the originally damaged DurableLog can be reused to create a new Repair Log or discarded if
 * the Segment Container recovers as expected.
 */
public class DurableDataLogRepairCommand extends DataRecoveryCommand {

    private final static Duration TIMEOUT = Duration.ofSeconds(10);
    @VisibleForTesting
    private List<LogEditOperation> durableLogEdits;
    private boolean testMode = false;

    /**
     * Creates a new instance of the DurableLogRepairCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableDataLogRepairCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Constructor for testing purposes. We can provide predefined edit commands as input and validate the results
     * without having to provide console input.
     *
     * @param args The arguments for the command.
     * @param durableLogEdits List of LogEditOperation to be done on the original log.
     */
    @VisibleForTesting
    DurableDataLogRepairCommand(CommandArgs args, List<LogEditOperation> durableLogEdits) {
        super(args);
        this.durableLogEdits = durableLogEdits;
        this.testMode = true;
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int containerId = getIntArg(0);
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val zkClient = createZKClient();
        @Cleanup
        val dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, this.executorService);
        dataLogFactory.initialize();

        // Open the Original Log in read-only mode.
        @Cleanup
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);
        @Cleanup
        val originalDataLogReadOnly = originalDataLog.asReadOnly();

        // Disable the Original Log, if not disabled already.
        output("Original DurableLog is enabled. You are about to disable it.");
        if (!this.testMode && !confirmContinue()) {
            output("Not disabling Original Log this time.");
            return;
        }
        originalDataLog.disable();

        // Create a new Backup Log to store the Original Log contents.
        @Cleanup
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(BACKUP_LOG_ID);
        backupDataLog.initialize(TIMEOUT);

        // Define the callbacks that will back up the Original Log contents into the Backup Log.
        @Cleanup
        BackupState backupState = new BackupState(backupDataLog, executorService);
        int operationsReadFromOriginalLog = 0;
        try {
            operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback(backupState, containerId, originalDataLogReadOnly);
            // Ugly, but it ensures that the last write done in the DataFrameBuilder is actually persisted.
            backupDataLog.append(new CompositeByteArraySegment(new byte[0]), TIMEOUT).join();
            // The number of processed operation should match the number of read operations from DebugRecoveryProcessor.
            assert backupState.getProcessedOperations() == operationsReadFromOriginalLog;
            assert !backupState.isFailed;
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Safely rollback here.
        }
        // Validate that the Original and Backup logs have the same number of operations.
        @Cleanup
        val validationBackupDataLog = dataLogFactory.createDebugLogWrapper(BACKUP_LOG_ID);
        @Cleanup
        val validationBackupDataLogReadOnly = validationBackupDataLog.asReadOnly();
        int backupLogReadOperations = readDurableDataLogWithCustomCallback((a, b) -> output("Reading: " + a), BACKUP_LOG_ID, validationBackupDataLogReadOnly);
        outputInfo("Original DurableLog operations read: " + operationsReadFromOriginalLog +
                ", Backup DurableLog operations read: " + backupLogReadOperations);

        // FIXME: We need to ensure that we read everything from original log once disabled. Currently the last entry is missing.
        assert operationsReadFromOriginalLog == backupLogReadOperations;

        output("Original DurableLog has been backed up correctly.");
        if (!this.testMode && !confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Get user input of operations to skip, replace, or delete.
        if (this.durableLogEdits == null) {
            this.durableLogEdits = getDurableLogEditsFromUser();
        }

        // Edit Operations need to be sorted, and they should involve only one actual Operations (e.g., we do not allow 2
        // edits on the same operation id).
        checkDurableLogEdits(this.durableLogEdits);

        // Show the edits to be committed to the original durable log so the user can confirm.
        output("The following edits will be used to edit the Original Log:");
        durableLogEdits.forEach(System.out::println);

        // Replace original log with the new contents.
        output("Ready to apply admin-provided changes to the Original Log.");
        if (!this.testMode && !confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Create a new Repair log to store the result of edits applied to the Original Log.
        @Cleanup
        DurableDataLog editedDataLog = dataLogFactory.createDurableDataLog(REPAIR_LOG_ID);
        editedDataLog.initialize(TIMEOUT);

        // Define the callbacks that will write the edited contents into the Repair Log.
        LogEditingState logEditState = new LogEditingState(editedDataLog, durableLogEdits, executorService);
        try {
            int readBackupLogAgain = readDurableDataLogWithCustomCallback(logEditState, BACKUP_LOG_ID, backupDataLog);
            // Ugly, but it ensures that the last write done in the DataFrameBuilder is actually persisted.
            editedDataLog.append(new CompositeByteArraySegment(new byte[0]), TIMEOUT).join();
            outputInfo("Backup DurableLog operations read on first attempt: " + backupLogReadOperations +
                    ", Backup DurableLog operations read on second attempt: " + readBackupLogAgain);
            assert !logEditState.isFailed;
        } catch (Exception ex) {
            outputError("There have been errors while creating the edited version of the DurableLog.");
            ex.printStackTrace();
            // TODO: Safely rollback here.
        }

        int editedDurableLogOperations = readDurableDataLogWithCustomCallback((op, list) -> System.out.println("EDITED LOG OPS: " + op), REPAIR_LOG_ID, editedDataLog);
        outputInfo("Edited DurableLog Operations read: " + editedDurableLogOperations);

        // Overwrite the original DurableLog metadata with the edited DurableLog metadata.
        @Cleanup
        val editedLogWrapper = dataLogFactory.createDebugLogWrapper(REPAIR_LOG_ID);
        output("Original DurableLog Metadata: " + originalDataLog.fetchMetadata());
        output("Edited DurableLog Metadata: " + editedLogWrapper.fetchMetadata());
        originalDataLog.forceMetadataOverWrite(editedLogWrapper.fetchMetadata());
        output("New Original DurableLog Metadata (after replacement): " + originalDataLog.fetchMetadata());

        // Read the edited contents that are now reachable from the original log id.
        try {
            @Cleanup
            val finalEditedLog = originalDataLog.asReadOnly();
            int finalEditedLogReadOps = readDurableDataLogWithCustomCallback((op, list) -> System.out.println(op), containerId, finalEditedLog);
            outputInfo("Edited DurableLog operations read: " + finalEditedLogReadOps);
        } catch (Exception ex) {
            outputError("Problem reading original DurableLog with edits.");
            ex.printStackTrace();
            // TODO: In case of failure, cleanup the backup log file
        }

        // TODO: Cleanup Edited and Backup Logs
    }

    @VisibleForTesting
    void checkDurableLogEdits(List<LogEditOperation> durableLogEdits) {
        // TODO: Add checks to durableLogEdits to validate that they are added correctly.
    }

    @VisibleForTesting
    List<LogEditOperation> getDurableLogEditsFromUser() {
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        boolean finishInputCommands = false;
        while (!finishInputCommands) {
            try {
                switch (getStringUserInput("Select edit action on DurableLog: [delete|add|replace]")) {
                    case "delete":
                        long initialOpId = getLongUserInput("Initial operation id to delete? (inclusive)");
                        long finalOpId = getLongUserInput("Initial operation id to delete? (exclusive)");
                        durableLogEdits.add(new LogEditOperation(LogEditType.DELETE_OPERATION, initialOpId, finalOpId, null));
                        break;
                    case "add":
                        initialOpId = getLongUserInput("At which Operation sequence number would you like to add new Operations?");
                        do {
                            durableLogEdits.add(new LogEditOperation(LogEditType.ADD_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                            outputInfo("You can add more Operations at this sequence number or not.");
                        } while (confirmContinue());
                        break;
                    case "replace":
                        initialOpId = getLongUserInput("What Operation sequence number would you like to replace?");
                        durableLogEdits.add(new LogEditOperation(LogEditType.REPLACE_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                        break;
                    default:
                        output("Invalid operation, please select one of [delete|add|replace]");
                }
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                ex.printStackTrace();
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                ex.printStackTrace();
            }
            outputInfo("You can continue adding edits to the original DurableLog.");
            finishInputCommands = confirmContinue();
        }

        return durableLogEdits;
    }

    @VisibleForTesting
    Operation createUserDefinedOperation() {
        final String operations = "[DeleteSegmentOperation|MergeSegmentOperation|MetadataCheckpointOperation|" +
                "StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|" +
                "StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]";
        switch (getStringUserInput("Type one of the following Operations to instantiate: " + operations)) {
            case "DeleteSegmentOperation":
                long segmentId = getLongUserInput("Introduce Segment Id for DeleteSegmentOperation:");
                return new DeleteSegmentOperation(segmentId);
            case "MergeSegmentOperation":
                long targetSegmentId = getLongUserInput("Introduce Target Segment Id for MergeSegmentOperation:");
                long sourceSegmentId = getLongUserInput("Introduce Source Segment Id for MergeSegmentOperation:");
                new MergeSegmentOperation(targetSegmentId, sourceSegmentId, createAttributeUpdateCollection());
                break;
            case "MetadataCheckpointOperation":
            case "StorageMetadataCheckpointOperation":
            case "StreamSegmentAppendOperation":
            case "StreamSegmentMapOperation":
                throw new UnsupportedOperationException();
            case "StreamSegmentSealOperation":
                segmentId = getLongUserInput("Introduce Segment Id for StreamSegmentSealOperation:");
                return new StreamSegmentSealOperation(segmentId);
            case "StreamSegmentTruncateOperation":
                segmentId = getLongUserInput("Introduce Segment Id for StreamSegmentTruncateOperation:");
                long offset = getLongUserInput("Introduce Offset for StreamSegmentTruncateOperation:");
                return new StreamSegmentTruncateOperation(segmentId, offset);
            case "UpdateAttributesOperation":
                segmentId = getLongUserInput("Introduce Segment Id for UpdateAttributesOperation:");
                return new UpdateAttributesOperation(segmentId, createAttributeUpdateCollection());
            default:
                output("Invalid operation, please select one of " + operations);
        }
        throw new UnsupportedOperationException();
    }

    private AttributeUpdateCollection createAttributeUpdateCollection() {
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        boolean finishInputCommands = false;
        while (!finishInputCommands) {
            output("Creating an AttributeUpdateCollection for this operation.");
            try {
                AttributeId attributeId = AttributeId.fromUUID(UUID.fromString(getStringUserInput("Introduce UUID for this AttributeUpdate: ")));
                AttributeUpdateType type = AttributeUpdateType.get((byte) getIntUserInput("Introduce AttributeUpdateType for this AttributeUpdate" +
                        "(0 (None), 1 (Replace), 2 (ReplaceIfGreater), 3 (Accumulate), 4(ReplaceIfEquals)): "));
                long value = getLongUserInput("Introduce the Value for this AttributeUpdate:");
                long comparisonValue = getLongUserInput("Introduce the comparison Value for this AttributeUpdate:");
                attributeUpdates.add(new AttributeUpdate(attributeId, type, value, comparisonValue));
            } catch(NumberFormatException ex){
                outputError("Wrong input argument.");
                ex.printStackTrace();
            } catch(Exception ex){
                outputError("Some problem has happened.");
                ex.printStackTrace();
            }
            outputInfo("You can continue adding AttributeUpdates to the AttributeUpdateCollection.");
            finishInputCommands = confirmContinue();
        }
        return attributeUpdates;
    }

    private int readDurableDataLogWithCustomCallback(BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> callback,
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
                containerConfig, readIndexConfig, this.executorService, logReaderCallbacks);
        int operationsRead = rp.performRecovery();
        output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
    }

    abstract static class AbstractLogProcessor implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>>, AutoCloseable {
        protected final AtomicLong processedOperations = new AtomicLong(0);
        protected final Map<Long, CompletableFuture<Void>> operationProcessingTracker = new ConcurrentHashMap<>();
        @NonNull
        protected final DataFrameBuilder<Operation> dataFrameBuilder;
        @Getter
        protected boolean isFailed = false;
        @Getter
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger beforeCommit = new AtomicInteger();
        private final AtomicInteger commitSuccess = new AtomicInteger();
        private final AtomicInteger commitFailure = new AtomicInteger();

        AbstractLogProcessor(DurableDataLog durableDataLog, ScheduledExecutorService executor) {
            DataFrameBuilder.Args args = new DataFrameBuilder.Args(a -> this.beforeCommit.getAndIncrement(),
                    b -> {
                        operationProcessingTracker.get(b.getLastFullySerializedSequenceNumber()).complete(null);
                        this.commitSuccess.getAndIncrement();
                    },
                    (c, d) -> {
                        operationProcessingTracker.get(d.getLastFullySerializedSequenceNumber()).complete(null);
                        isFailed = true; // Consider a single failed write as a failure in the whole process.
                        commitFailure.getAndIncrement();
                    },
                    executor);
            this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        }

        protected void trackOperation(Operation operation) {
            CompletableFuture<Void> confirmedWrite = new CompletableFuture<>();
            this.operationProcessingTracker.put(operation.getSequenceNumber(), confirmedWrite);
        }

        protected void waitForOperationCommit(Operation operation) {
            this.operationProcessingTracker.get(operation.getSequenceNumber()).join();
        }

        protected void writeAndConfirm(Operation operation) throws IOException {
            trackOperation(operation);
            this.dataFrameBuilder.append(operation);
            this.dataFrameBuilder.flush();
            waitForOperationCommit(operation);
        }

        public long getProcessedOperations() {
            return processedOperations.get();
        }

        public void close() {
            this.dataFrameBuilder.flush();
            this.dataFrameBuilder.close();
        }
    }

    class BackupState extends AbstractLogProcessor {

        BackupState(DurableDataLog backupDataLog, ScheduledExecutorService executor) {
            super(backupDataLog, executor);
        }

        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                writeAndConfirm(operation);
                processedOperations.incrementAndGet();
            } catch (Exception e) {
                outputError("Error serializing operation " + operation);
                e.printStackTrace();
                isFailed = true;
            }
        }
    }

    class LogEditingState extends AbstractLogProcessor {
        @NonNull
        private final List<LogEditOperation> durableLogEdits;
        private long newSequenceNumber = 1;

        LogEditingState(DurableDataLog editedDataLog, List<LogEditOperation> durableLogEdits, ScheduledExecutorService executor) {
            super(editedDataLog, executor);
            this.durableLogEdits = durableLogEdits;
        }

        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                // Nothing to edit, just write the Operations from the original log to the edited one.
                if (!hasEditsToApply(operation)) {
                    operation.resetSequenceNumber(newSequenceNumber++);
                    writeAndConfirm(operation);
                } else {
                    // Edits to a DurableLog are sorted by their initial operation id and they are removed once they
                    // have been applied. The only case in which we can find a DurableLog Operation with a sequence
                    // number lower than the next DurableLog edit is that the data corruption issue we are trying to
                    // repair induces duplication of DataFrames.
                    if (operation.getSequenceNumber() < durableLogEdits.get(0).getInitialOperationId()) {
                        outputError("Found an Operation with a lower sequence number than the initial" +
                                "id of the next edit to apply. This may be symptom of a duplicated DataFrame and will" +
                                "also duplicate the associated edit: " + operation);
                        if (!confirmContinue()) {
                            output("Not editing original DurableLog for this operation.");
                            return;
                        }
                    }
                    // We have edits to do.
                    LogEditOperation logEdit = durableLogEdits.get(0);
                    switch (logEdit.getType()) {
                        case DELETE_OPERATION:
                            // A Delete Edit Operation on a DurableLog consists of not writing the range of Operations
                            // between its initial (inclusive) and final (exclusive) operation id.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          delete 2-4
                            //          Result Log = [1, 2 (4), 3 (5)] (former operation sequence number)
                            applyDeleteEditOperation(operation, logEdit);
                            break;
                        case ADD_OPERATION:
                            // An Add Edit Operation on a DurableLog consists of appending the desired Operation
                            // encapsulated in the Add Edit Operation before the actual Operation contained in the log.
                            // Note that we may want to add multiple new Operations at a specific position before the
                            // actual one.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          add 2, opA, opB, opC
                            //          Result Log = [1, 2 (opA), 3 (opB), 4 (opC), 5 (2), 6 (3), 7 (4), 8 (5)] (former operation sequence number)
                            applyAddEditOperation(operation, logEdit);
                            break;
                        case REPLACE_OPERATION:
                            // A Replace Edit Operation on a DurableLog consists of deleting the current Operation and
                            // adding the new Operation encapsulated in the Replace Edit Operation.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          replace 2, opA
                            //          Result Log = [1, 2 (opA), 3, 4, 5] (former operation sequence number)
                            applyReplaceEditOperation(logEdit);
                            break;
                        default:
                            outputError("Unknown DurableLog edit type: " + durableLogEdits.get(0).getType());
                    }
                }
            } catch (Exception e) {
                outputError("Error serializing operation " + operation);
                e.printStackTrace();
                isFailed = true;
            }
        }

        private void applyReplaceEditOperation(LogEditOperation logEdit) throws IOException {
            logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
            writeAndConfirm(logEdit.getNewOperation());
            durableLogEdits.remove(0);
        }

        private void applyAddEditOperation(Operation operation, LogEditOperation logEdit) throws IOException {
            long currentInitialAddOperation = logEdit.getInitialOperationId();
            while (!durableLogEdits.isEmpty() && logEdit.getType().equals(LogEditType.ADD_OPERATION)
                    && logEdit.getInitialOperationId() == currentInitialAddOperation) {
                logEdit = durableLogEdits.get(0);
                logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
                writeAndConfirm(logEdit.getNewOperation());
                durableLogEdits.remove(0);
            }
            // After all the additions are done, add the current log operation.
            operation.resetSequenceNumber(newSequenceNumber++);
            writeAndConfirm(operation);
        }

        private void applyDeleteEditOperation(Operation operation, LogEditOperation logEdit) {
            outputInfo("Deleting operation from DurableLog: " + operation);
            if (logEdit.getFinalOperationId() == operation.getSequenceNumber() + 1) {
                // Once reached the end of the Delete Edit Operation range, remove it from the list.
                try {
                    durableLogEdits.remove(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                outputInfo("Completed Delete Edit Operation on DurableLog: " + logEdit);
            }
        }

        private boolean hasEditsToApply(Operation op) {
            if (this.durableLogEdits.isEmpty()) {
                return false;
            }
            LogEditType editType = durableLogEdits.get(0).getType();
            long editInitialOpId = durableLogEdits.get(0).getInitialOperationId();
            long editFinalOpId = durableLogEdits.get(0).getFinalOperationId();
            return editInitialOpId == op.getSequenceNumber()
                    || (editType.equals(LogEditType.DELETE_OPERATION)
                        && editInitialOpId <= op.getSequenceNumber()
                        && editFinalOpId >= op.getSequenceNumber());
        }
    }

    enum LogEditType {
        DELETE_OPERATION,
        ADD_OPERATION,
        REPLACE_OPERATION
    }

    @Data
    static class LogEditOperation {
        private final LogEditType type;
        private final long initialOperationId;
        private final long finalOperationId;
        private final Operation newOperation;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-repair", "Allows to replace the data of a" +
                "DurableLog by an edited version of it in the case that some entries are damaged.");
    }
}
