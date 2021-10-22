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
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.*;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * This command provides an administrator with the basic primitives to manipulate a DurableLog with damaged entries.
 * The workflow of this command is as follows:
 * 1. Disable the original DurableLog (if not done yet).
 * 2. Reads the original damaged DurableLog and creates a backup copy of it for safety reasons.
 * 3. Validate and buffer all the edits from admin to be done on the original DurableLog data (i.e., skip, delete,
 * replace operations). All these changes are then written on a Repair Log (i.e., original DurableLog data + admin changes).
 * 4. With the desired updates written in the Repair Log, the admin can replace the original DurableLog by the Repair
 * Log. This will make the DurableLog for the Segment Container under repair to point to the Repair Log data.
 * 5. The backed-up data for the originally damaged DurableLog can be reused to create a new Repair Log or discarded if
 * the Segment Container recovers as expected.
 */
public class DurableDataLogRepairCommand extends DataRecoveryCommand {

    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * Creates a new instance of the DurableLogRepairCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableDataLogRepairCommand(CommandArgs args) {
        super(args);
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

        // Open the original data log in read-only mode.
        @Cleanup
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);
        @Cleanup
        val bkLog = originalDataLog.asReadOnly();

        // Disable the original log, if not disabled already.
        originalDataLog.disable();

        // Create a new data log to store the original log contents.
        @Cleanup
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(BACKUP_LOG_ID);
        backupDataLog.initialize(TIMEOUT);
        DataFrameBuilder.Args args = new DataFrameBuilder.Args(a -> {}, b -> {}, (c, d) -> {}, executorService);
        @Cleanup
        DataFrameBuilder<Operation> dataFrameBuilderBackup = new DataFrameBuilder<>(backupDataLog, OperationSerializer.DEFAULT, args);

        // Define the callbacks that will backup the original contents into the backup log.
        BackupState backupState = new BackupState(dataFrameBuilderBackup);
        int operationsReadFromOriginalLog = 0;
        try {
            operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback(backupState, containerId, bkLog);
            assert !backupState.isFailed;
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Safely rollback here.
        }

        // Validate that the original and backup logs have the same number of operations.
        int backupLogReadOperations = readDurableDataLogWithCustomCallback((a, b) -> {}, containerId, backupDataLog);
        System.err.println("ORIGINAL LOG OPERATIONS: " + operationsReadFromOriginalLog +
                " BACKUP LOG OPERATIONS: " + backupLogReadOperations);
        // FIXME: We need to ensure that we read everything from original log once disabled (flushing?)
        //assert operationsReadFromOriginalLog == readDurableDataLogWithCustomCallback((a, b) -> {}, containerId, backupDataLog);

        output("Original DurableLog has been backed up correctly.");
        if (!confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Get user input of operations to skip, replace, or delete.
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        durableLogEdits.add(new LogEditOperation(LogEditType.REPLACE_OPERATION, 1, 1, new DeleteSegmentOperation(123)));
        durableLogEdits.add(new LogEditOperation(LogEditType.DELETE_OPERATION, 2, 1000, null));
        /*durableLogEdits.add(new LogEditOperation(LogEditType.ADD_OPERATION, 1, 200, new DeleteSegmentOperation(123)));
        durableLogEdits.add(new LogEditOperation(LogEditType.ADD_OPERATION, 300, 300, new DeleteSegmentOperation(123)));//getDurableLogEditesFromUser();*/

        // Operations need to be sorted and they should impact on disjoint operations (e.g., we cannot allow 2 edits on
        // the same operation id)
        // TODO: Add checks to durableLogEdits to validate that they are added correctly.

        // Show the edits to be committed to the original durable log so the user can confirm.
        output("The following edits will be committed to original DurableLog:");
        durableLogEdits.forEach(System.out::println);

        // Replace original log with the new contents.
        output("Ready to apply admin-provided changes in the original DurableLog.");
        if (false) { //!confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Create a new data log to store the result of edits on original data log.
        @Cleanup
        DurableDataLog editedDataLog = dataLogFactory.createDurableDataLog(REPAIR_LOG_ID);
        editedDataLog.initialize(TIMEOUT);
        @Cleanup
        DataFrameBuilder<Operation> dataFrameBuilderEdit = new DataFrameBuilder<>(editedDataLog, OperationSerializer.DEFAULT, args);

        // Define the callbacks that will backup the original contents into the backup log.
        LogEditingState logEditState = new LogEditingState(dataFrameBuilderEdit, durableLogEdits);
        try {
            int readBackupLogAgain = readDurableDataLogWithCustomCallback(logEditState, BACKUP_LOG_ID, backupDataLog);
            dataFrameBuilderEdit.flush();
            System.err.println("BACKUP LOG OPERATIONS: " + backupLogReadOperations +
                    " NEW BACKUP LOG OPERATIONS: " + readBackupLogAgain);
            assert !logEditState.isFailed;
        } catch (Exception ex) {
            outputError("There have been errors while creating the edited version of the DurableLog.");
            ex.printStackTrace();
            // TODO: Safely rollback here.
        }

        // Overwrite the original DurableLog metadata with the edited DurableLog metadata.
        @Cleanup
        val editedLogWrapper = dataLogFactory.createDebugLogWrapper(REPAIR_LOG_ID);
        originalDataLog.forceMetadataOverWrite(editedLogWrapper.fetchMetadata());

        // Read the edited contents that are now reachable from the original log id.
        try {
            @Cleanup
            val finalEditedLog = dataLogFactory.createDebugLogWrapper(containerId).asReadOnly();
            int finalEditedLogReadOps = readDurableDataLogWithCustomCallback((op, list) -> System.out.println(op), containerId, finalEditedLog);
            System.err.println("BACKUP LOG OPERATIONS: " + backupLogReadOperations +
                    " FINAL EDITED LOG OPERATIONS: " + finalEditedLogReadOps);
        } catch (Exception ex) {
            outputError("Problem reading original DurableLog with edits.");
            ex.printStackTrace();
            // TODO: In case of failure, cleanup the backup log file
        }
    }

    public int readDurableDataLogWithCustomCallback(BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> callback,
                                                     int containerId, DurableDataLog durableDataLog) throws Exception {
        val backupCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                callback,
                op -> false,
                null,
                null);
        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);
        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, durableDataLog,
                containerConfig, readIndexConfig, this.executorService, backupCallbacks);
        int operationsRead = rp.performRecovery();
        output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
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

    private Operation createUserDefinedOperation() {
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

    class BackupState implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> {

        @NonNull
        private final DataFrameBuilder<Operation> backupLogFrameBuilder;
        private boolean isFailed = false;

        BackupState(DataFrameBuilder<Operation> backupLogFrameBuilder) {
            this.backupLogFrameBuilder = backupLogFrameBuilder;
        }

        public void accept(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                System.err.println("Backing up " + op);
                backupLogFrameBuilder.append(op);
                backupLogFrameBuilder.flush();
            } catch (Exception e) {
                outputError("Error serializing operation " + op);
                e.printStackTrace();
                isFailed = true;
            }
        }
    }

    class LogEditingState implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> {
        @NonNull
        private final DataFrameBuilder<Operation> editedDataLog;
        @NonNull
        private final List<LogEditOperation> durableLogEdits;
        private long newSequenceNumber = 1;
        private boolean isFailed = false;

        LogEditingState(DataFrameBuilder<Operation> editedDataLog, List<LogEditOperation> durableLogEdits) {
            this.editedDataLog = editedDataLog;
            this.durableLogEdits = durableLogEdits;
        }

        public void accept(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                // Nothing to edit, just write the Operations from the original log to the edited one.
                if (!hasToApplyEdits(op)) {
                    op.resetSequenceNumber(newSequenceNumber++);
                    editedDataLog.append(op);
                } else {
                    // Edits to a DurableLog are sorted by their initial operation id and they are removed once they
                    // have been applied. The only case in which we can find a DurableLog Operation with a sequence
                    // number lower than the next DurableLog edit is that the data corruption issue we are trying to
                    // repair induces duplication of DataFrames.
                    if (op.getSequenceNumber() < durableLogEdits.get(0).getInitialOperationId()) {
                        outputError("Found an Operation with a lower sequence number than the initial" +
                                "id of the next edit to apply. This may be symptom of a duplicated DataFrame and will" +
                                "also duplicate the associated edit: " + op);
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
                            outputInfo("Deleting operation from DurableLog: " + op);
                            if (logEdit.getFinalOperationId() == op.getSequenceNumber() + 1) {
                                // Once reached the end of the Delete Edit Operation range, remove it from the list.
                                try {
                                    durableLogEdits.remove(0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                outputInfo("Completed Delete Edit Operation on DurableLog: " + logEdit);
                            }
                            break;
                        case ADD_OPERATION:
                            // An Add Edit Operation on a DurableLog consists of appending the desired Operation
                            // encapsulated in the Add Edit Operation before the actual Operation contained in the log.
                            // Note that we may want to add multiple new Operations at a specific position before the
                            // actual one.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          add 2, opA, opB, opC
                            //          Result Log = [1, 2 (opA), 3 (opB), 4 (opC), 5 (2), 6 (3), 7 (4), 8 (5)] (former operation sequence number)
                            long currentInitialAddOperation = logEdit.getInitialOperationId();
                            while (!durableLogEdits.isEmpty() && logEdit.getType().equals(LogEditType.ADD_OPERATION)
                                    && logEdit.getInitialOperationId() == currentInitialAddOperation) {
                                logEdit = durableLogEdits.get(0);
                                logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
                                editedDataLog.append(logEdit.getNewOperation());
                                durableLogEdits.remove(0);
                            }
                            // After all the additions are done, add the current log operation.
                            op.resetSequenceNumber(newSequenceNumber++);
                            editedDataLog.append(op);
                            break;
                        case REPLACE_OPERATION:
                            // A Replace Edit Operation on a DurableLog consists of deleting the current Operation and
                            // adding the new Operation encapsulated in the Replace Edit Operation.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          replace 2, opA
                            //          Result Log = [1, 2 (opA), 3, 4, 5] (former operation sequence number)
                            logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
                            editedDataLog.append(logEdit.getNewOperation());
                            editedDataLog.flush();
                            durableLogEdits.remove(0);
                            break;
                        default:
                            outputError("Unknown DurableLog edit type: " + durableLogEdits.get(0).getType());
                    }
                }
            } catch (Exception e) {
                outputError("Error serializing operation " + op);
                e.printStackTrace();
                isFailed = true;
            }
        }

        private boolean hasToApplyEdits(Operation op) {
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
