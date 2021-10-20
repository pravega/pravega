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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);

        // Open the original data log in read-only mode.
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);
        val bkLog = originalDataLog.asReadOnly();

        // Create a new data log to store the original log contents.
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(Integer.MAX_VALUE);
        backupDataLog.initialize(Duration.ofSeconds(10)); //TODO: Fix this duration
        DataFrameBuilder.Args args = new DataFrameBuilder.Args(a -> {}, b -> {}, (c, d) -> {}, executorService);
        @Cleanup
        DataFrameBuilder<Operation> dataFrameBuilderBackup = new DataFrameBuilder<>(backupDataLog, OperationSerializer.DEFAULT, args);

        // Define the callbacks that will backup the original contents into the backup log.
        BackupState backupState = new BackupState(dataFrameBuilderBackup);
        val backupCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                backupState,
                op -> false,
                null,
                null);

        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, bkLog,
                containerConfig, readIndexConfig, this.executorService, backupCallbacks);
        try {
            output("Number of operations backed up from original log: " + rp.performRecovery());
            assert !backupState.isFailed;
        } catch (Exception ex) {
            outputError("Backup of original log failed.");
            ex.printStackTrace();
            originalDataLog.close();
            bkLog.close();
            backupDataLog.close();
            throw ex;
            // TODO: In case of failure, cleanup the backup log file
        }

        // Validate that the original and backup logs are actually the same.
        // TODO: Validate backup log

        output("Original DurableLog has been backed up correctly.");
        if (!confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Get user input of operations to skip, replace, or delete.
        List<LogEditOperation> durableLogEdits = Collections.singletonList(new LogEditOperation(LogEditType.DELETE_OPERATION, 0, 0, null));//getDurableLogEditesFromUser();

        // Operations need to be sorted and they should impact on disjoint operations (e.g., we cannot allow 2 edits on
        // the same operation id)
        // TODO: Add checks to durableLogEdits

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
        DurableDataLog editedDataLog = dataLogFactory.createDurableDataLog(Integer.MAX_VALUE - 1);
        editedDataLog.initialize(Duration.ofSeconds(10)); //TODO: Fix this duration
        @Cleanup
        DataFrameBuilder<Operation> dataFrameBuilderEdit = new DataFrameBuilder<>(editedDataLog, OperationSerializer.DEFAULT, args);

        // Define the callbacks that will backup the original contents into the backup log.
        LogEditingState logEditState = new LogEditingState(dataFrameBuilderEdit, durableLogEdits);
        val editCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                logEditState,
                op -> false,
                null,
                null);

        @Cleanup
        val rp2 = DebugRecoveryProcessor.create(Integer.MAX_VALUE, backupDataLog,
                containerConfig, readIndexConfig, this.executorService, editCallbacks);
        try {
            output("Number of operations backed up from original log: " + rp2.performRecovery());
            assert !logEditState.isFailed;
        } catch (Exception ex) {
            outputError("Backup of original log failed.");
            ex.printStackTrace();
            // TODO: In case of failure, cleanup the backup log file
        } finally {
            originalDataLog.close();
            bkLog.close();
            backupDataLog.close();
        }

        // In case that edits have been applied successfully, replace the metadata of the original DurableLog with the
        // edited Durable Log.
        if (!logEditState.isFailed) {
            outputInfo("Edited DurableLog created successfully. Replacing metadata of original DurableLog.");
        } else {
            outputError("There have been errors while creating the edited version of the DurableLog.");
        }
    }

    private List<LogEditOperation> getDurableLogEditesFromUser() {
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        boolean finishInputCommands = false;
        while (!finishInputCommands) {
            try {
                switch ("delete") { //getStringUserInput("Select edit action on DurableLog: [delete|add|replace]")) {
                    case "delete":
                        long initialOpId = 0; //getLongUserInput("Initial operation id to delete? (inclusive)");
                        long finalOpId = 10; //getLongUserInput("Initial operation id to delete? (exclusive)");
                        durableLogEdits.add(new LogEditOperation(LogEditType.DELETE_OPERATION, initialOpId, finalOpId, null));
                        break;
                    case "add":
                    case "replace":
                        break;
                    default:
                        output("Invalid operation, please select one of [delete|add|replace]");
                }
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument, try again.");
                ex.printStackTrace();
            }
            finishInputCommands = true; //confirmContinue();
        }

        return durableLogEdits;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-repair", "Allows to replace the data of a" +
                "DurableLog by an edited version of it in the case that some entries are damaged.");
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
        private long newSequenceNumber = 0;
        private boolean isFailed = false;

        LogEditingState(DataFrameBuilder<Operation> editedDataLog, List<LogEditOperation> durableLogEdits) {
            this.editedDataLog = editedDataLog;
            this.durableLogEdits = durableLogEdits;
        }

        public void accept(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                // Nothing to edit, just write the Operations from the original log to the edited one.
                if (!hasToApplyEdits(op)) {
                    //op.setSequenceNumber(newSequenceNumber++); // FIXME: seems not possible to rewrite op id
                    editedDataLog.append(op);
                    editedDataLog.flush();
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
                    switch (durableLogEdits.get(0).getType()) {
                        case DELETE_OPERATION:
                            // A Delete Edit Operation on a DurableLog consists of not writing the range of Operations
                            // between its initial (inclusive) and final (exclusive) operation id.
                            outputInfo("Deleting operation from DurableLog: " + op);
                            if (durableLogEdits.get(0).getFinalOperationId() + 1 == op.getSequenceNumber()) {
                                // Once reached the end of the Delete Edit Operation range, remove it from the list.
                                durableLogEdits.remove(0);
                                outputInfo("Completed Delete Edit Operation on DurableLog: " + durableLogEdits.get(0));
                            }
                            break;
                        case ADD_OPERATION:
                            break;
                        case REPLACE_OPERATION:
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
                        && editInitialOpId > op.getSequenceNumber()
                        && editFinalOpId < op.getSequenceNumber());
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
}
