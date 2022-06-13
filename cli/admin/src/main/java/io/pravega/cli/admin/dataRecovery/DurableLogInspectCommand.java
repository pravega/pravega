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
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * This command provides an administrator with the basic primitives to inspect a DurableLog.
 * The workflow of this command is as follows:
 * 1. Checks if the Original Log is disabled (exit otherwise).
 * 2. Reads the original damaged DurableLog and creates a backup copy of it for safety reasons.
 * 3. User input for conditions to inspect.
 * 4. Result list is saved in a text file.
 */
public class DurableLogInspectCommand extends DurableDataLogRepairCommand {

    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * Creates a new instance of the DurableLogInspectCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableLogInspectCommand(CommandArgs args) {
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
        DurableDataLogFactory dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, getCommandArgs().getState().getExecutor());
        dataLogFactory.initialize();

        // Open the Original Log in read-only mode.
        @Cleanup
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);

        // Make sure that the reserved id for Backup log is free before making any further progress.
        boolean createNewBackupLog = true;
        if (existsBackupLog(dataLogFactory)) {
            output("We found data in the Backup log, probably from a previous repair operation (or someone else running the same command at the same time). " +
                    "You have three options: 1) Delete existing Backup Log and start a new repair process, " +
                    "2) Keep existing Backup Log and re-use it for the current repair (i.e., skip creating a new Backup Log), " +
                    "3) Quit.");
            switch (getIntUserInput("Select an option: [1|2|3]")) {
                case 1:
                    // Delete everything related to the old Backup Log.
                    try (DebugDurableDataLogWrapper backupDataLogDebugLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId())) {
                        backupDataLogDebugLogWrapper.deleteDurableLogMetadata();
                    }
                    break;
                case 2:
                    // Keeping existing Backup Log, so not creating a new one.
                    createNewBackupLog = false;
                    break;
                default:
                    output("Not doing anything with existing Backup Log this time.");
                    return;
            }
        }

        // Create a new Backup Log if there wasn't any or if we removed the existing one.
        if (createNewBackupLog) {
            createBackupLog(dataLogFactory, containerId, originalDataLog);
        }

        int backupLogReadOperations = validateBackupLog(dataLogFactory, containerId, originalDataLog, createNewBackupLog);

        output("Total reads original:" + backupLogReadOperations);
        // Get user input of operations to skip, replace, or delete.
        Predicate<String> durableLogPredicates = getConditionTypeFromUser();

        backupLogReadOperations = filterResult(dataLogFactory, durableLogPredicates);
        // Show the edits to be committed to the original durable log so the user can confirm.
        output("Total reads :" + backupLogReadOperations);

        // Output as per the predicates present

        output("Process completed successfully! (You still need to enable the Durable Log so Pravega can use it)");
    }

    private int filterResult(DurableDataLogFactory dataLogFactory, Predicate<String> predicate) throws Exception {
        AtomicInteger res = new AtomicInteger();
        @Cleanup
        val validationBackupDataLog = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId());
        @Cleanup
        val validationBackupDataLogReadOnly = validationBackupDataLog.asReadOnly();
        readDurableDataLogWithCustomCallback((a, b) -> {
                    if (predicate.test(a.toString())) {
                        output(a.toString());
                        res.getAndIncrement();
                    }
                }, dataLogFactory.getBackupLogId(), validationBackupDataLogReadOnly);
        return res.get();
    }


    /**
     * Guides the users to a set of options for creating predicates that will eventually modify the
     * contents of the Original Log.
     *
     * @return List of predicates.
     */
    @VisibleForTesting
    Predicate<String> getConditionTypeFromUser() {
        Predicate<String> predicate = null;
        List<Predicate<String>> predicates = new ArrayList<>();
        boolean finishInputCommands = false;
        boolean next = false;
        String clause = "";
        while (!finishInputCommands) {
            try {
                if (next) {
                    clause = getStringUserInput("Select conditional operator: [and/or]");
                }
                final String operationTpe = getStringUserInput("Select condition type to display the output: [OperationType/SequenceNumber/SegmentId/Offset/Length/Attributes]");
                switch (operationTpe) {
                    case "OperationType":
                        String op = getStringUserInput("Enter valid operation type: [DeleteSegmentOperation|MergeSegmentOperation|MetadataCheckpointOperation|\" +\n" +
                                "                \"StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|\" +\n" +
                                "                \"StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]");
                        predicates.add( a -> a.contains(op));
                        break;
                    case "SequenceNumber":
                        long in = getLongUserInput("Valid Sequence Number: ");
                        predicates.add( a -> a.contains(Long.toString(in)));
                        break;
                    case "SegmentId":
                        in = getLongUserInput("Valid segmentId to search: ");
                        predicates.add( a -> a.contains(Long.toString(in)));
                        break;
                    case "Offset":
                        in = getLongUserInput("IValid offset to seach: ");
                        predicates.add( a -> a.contains(Long.toString(in)));
                        break;
                    case "Length":
                        in = getLongUserInput("IValid length to seach: ");
                        predicates.add( a -> a.contains(Long.toString(in)));
                        break;
                    case "Attributes":
                        in = getLongUserInput("Valid number of attributes to seach: ");
                        predicates.add( a -> a.contains(Long.toString(in)));
                        break;
                    default:
                        output("Invalid operation, please select one of [delete|add|replace]");
                }
                predicate = clause.equals("and") ?
                        predicates.stream().reduce(Predicate::and).orElse( x -> true) : predicates.stream().reduce(Predicate::or).orElse( x -> false);
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                outputException(ex);
            } catch (IllegalStateException ex) {
                // Last input was incorrect, so remove it.
                output("Last Log Inspect Operation did not pass the checks, removing it from list.");
                //durableLogEdits.remove(durableLogEdits.size() - 1);
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
            output("You can continue adding conditions for inspect.");
            finishInputCommands = !confirmContinue();
            next = !finishInputCommands;
        }
        output("Value of predicates is : " + predicates);
        return predicate;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-inspect", "Allows to inspect DurableLog " +
                "damaged/corrupted Operations.",
                new ArgDescriptor("container-id", "Id of the Container to inspect."));
    }
}
