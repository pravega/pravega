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
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.*;

import java.time.Duration;
import java.util.ArrayList;
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

        // Define the callbacks that will backup the original contents into the backup log.
        BackupState backupState = new BackupState(backupDataLog);
        val callbacks = new DebugRecoveryProcessor.OperationCallbacks(
                backupState,
                op -> false,
                null,
                null);

        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, bkLog,
                containerConfig, readIndexConfig, this.executorService, callbacks);
        try {
            output("Number of operations backed up from original log: " + rp.performRecovery());
            assert !backupState.isFailed;
        } catch (Exception ex) {
            outputError("Backup of original log failed.");
            ex.printStackTrace();
            // TODO: In case of failure, cleanup the backup log file
        } finally {
            originalDataLog.close();
            bkLog.close();
            backupDataLog.close();
        }

        // Validate that the original and backup logs are actually the same.
        // TODO: Validate backup log

        output("Original DurableLog has been backed up correctly.");
        if (!confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Get user input of operations to skip, replace, or delete.
        boolean finishInputCommands = false;
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        while (!finishInputCommands) {
            try {
                switch (getStringUserInput("Select edit action on DurableLog: [delete|add|replace]")) {
                    case "delete":
                        long initialOpId = getLongUserInput("Initial operation id to delete? (inclusive)");
                        long finalOpId = getLongUserInput("Initial operation id to delete? (exclusive)");
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
            finishInputCommands = confirmContinue();
        }

        // Operations need to be sorted and they should impact on disjoint operations (e.g., we cannot allow 2 edits on
        // the same operation id)
        // TODO: Add checks to durableLogEdits

        // Show the edits to be committed to the original durable log so the user can confirm.
        output("The following edits will be committed to original DurableLog:");
        durableLogEdits.forEach(System.out::println);

        // Replace original log with the new contents.
        output("Ready to apply admin-provided changes in the original DurableLog.");
        if (!confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Read from backup log and apply commands and write entries.
        val originalDataLogToEdit = dataLogFactory.createDebugLogWrapper(containerId);

    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-repair", "Allows to replace the data of a" +
                "DurableLog by an edited version of it in the case that some entries are damaged.");
    }

    class BackupState implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> {

        @NonNull
        private final DurableDataLog backupLog;
        private boolean isFailed = false;

        BackupState(DurableDataLog durableDataLog) {
            this.backupLog = durableDataLog;
        }

        public void accept(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                output("Serializing and writing operation: " + op);
                ByteArraySegment serializedOp = OperationSerializer.DEFAULT.serialize(op);
                CompositeArrayView arrayView = new CompositeByteArraySegment(serializedOp.getLength()); // FIXME: Better way to initialize this
                arrayView.copyFrom(serializedOp.getBufferViewReader(), 0, serializedOp.getLength());
                this.backupLog.append(arrayView, Duration.ofSeconds(30)).join();
            } catch (Exception e) {
                outputError("Error serializing operation " + op);
                e.printStackTrace();
                isFailed = true;
            }
        }
    }

    class LogEditingState implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> {

        @NonNull
        private final DurableDataLog originalLog;
        private final List<LogEditOperation> durableLogEdits;
        private boolean isFailed = false;

        LogEditingState(DurableDataLog durableDataLog, List<LogEditOperation> durableLogEdits) {
            this.originalLog = durableDataLog;
            this.durableLogEdits = durableLogEdits;
        }

        public void accept(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                output("Serializing and writing operation: " + op);
                ByteArraySegment serializedOp = OperationSerializer.DEFAULT.serialize(op);
                CompositeArrayView arrayView = new CompositeByteArraySegment(serializedOp.getLength()); // FIXME: Better way to initialize this
                arrayView.copyFrom(serializedOp.getBufferViewReader(), 0, serializedOp.getLength());
                this.originalLog.append(arrayView, Duration.ofSeconds(30)).join();
            } catch (Exception e) {
                outputError("Error serializing operation " + op);
                e.printStackTrace();
                isFailed = true;
            }
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
