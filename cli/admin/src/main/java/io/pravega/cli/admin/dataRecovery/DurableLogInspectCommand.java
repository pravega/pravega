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
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationInspectInfo;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.val;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;

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

        int durableLogReadOperations = readDurableDataLog( containerId, originalDataLog);

        output("Total reads original:" + durableLogReadOperations);

        Predicate<OperationInspectInfo> durableLogPredicates = getConditionTypeFromUser();

        durableLogReadOperations = filterResult(durableLogPredicates, containerId, originalDataLog);
        // Show the edits to be committed to the original durable log so the user can confirm.
        output("Total reads matching conditions :" + durableLogReadOperations);

        // Output as per the predicates present

        output("Process completed successfully!!");
    }

    protected int readDurableDataLog(int containerId, DebugDurableDataLogWrapper originalDataLog) throws Exception {

        int operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback((a, b) ->  {
            output("Reading: " + getActualOperation(a));
            }, containerId, originalDataLog.asReadOnly());
        return operationsReadFromOriginalLog;
    }

    private OperationInspectInfo getActualOperation(Operation op) {
        OperationInspectInfo res = null;
        if (op instanceof StreamSegmentAppendOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((StreamSegmentAppendOperation) op).getStreamSegmentId(), ((StreamSegmentAppendOperation) op).getStreamSegmentOffset(),
                    ((StreamSegmentAppendOperation) op).getAttributeUpdates().size());
        } else if (op instanceof StreamSegmentSealOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((StreamSegmentSealOperation) op).getStreamSegmentId(), ((StreamSegmentSealOperation) op).getStreamSegmentOffset(),
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof MergeSegmentOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((MergeSegmentOperation) op).getStreamSegmentId(), ((MergeSegmentOperation) op).getStreamSegmentOffset(),
                    ((MergeSegmentOperation) op).getAttributeUpdates().size());
        } else if (op instanceof UpdateAttributesOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((UpdateAttributesOperation) op).getStreamSegmentId(), OperationInspectInfo.DEFAULT_ABSENT_VALUE,
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof StreamSegmentTruncateOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((StreamSegmentTruncateOperation) op).getStreamSegmentId(), ((StreamSegmentTruncateOperation) op).getStreamSegmentOffset(),
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof DeleteSegmentOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((DeleteSegmentOperation) op).getStreamSegmentId(), ((DeleteSegmentOperation) op).getStreamSegmentOffset(),
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof MetadataCheckpointOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE, OperationInspectInfo.DEFAULT_ABSENT_VALUE, OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof StorageMetadataCheckpointOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    OperationInspectInfo.DEFAULT_ABSENT_VALUE, OperationInspectInfo.DEFAULT_ABSENT_VALUE, OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        } else if (op instanceof StreamSegmentMapOperation) {
            res = new OperationInspectInfo(op.getSequenceNumber(), op.getClass().getSimpleName(), op.getCacheLength(),
                    ((StreamSegmentMapOperation) op).getStreamSegmentId(), OperationInspectInfo.DEFAULT_ABSENT_VALUE, OperationInspectInfo.DEFAULT_ABSENT_VALUE);
        }
        return res;
    }

    private int filterResult(Predicate<OperationInspectInfo> predicate, int containerId, DebugDurableDataLogWrapper originalDataLog) throws Exception {
        AtomicInteger res = new AtomicInteger();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yy.HH-mm-ss");
        Date date = new Date();
        @Cleanup
        FileWriter writer = new FileWriter(createFileAndDirectory("DurableLogInspectResult" + dateFormat.format(date)));

        readDurableDataLogWithCustomCallback((a, b) -> {
                    if (predicate.test(getActualOperation(a))) {
                        output(a.toString());
                        try {
                            writer.write(a.toString());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        res.getAndIncrement();
                    }
                }, containerId, originalDataLog.asReadOnly());
        writer.flush();
        return res.get();
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
        //output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
    }

    /**
     * Guides the users to a set of options for creating predicates that will eventually modify the
     * contents of the Original Log.
     *
     * @return List of predicates.
     */
    @VisibleForTesting
    Predicate<OperationInspectInfo> getConditionTypeFromUser() {
        Predicate<OperationInspectInfo> predicate = null;
        List<Predicate<OperationInspectInfo>> predicates = new ArrayList<>();
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
                                " \"StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|\" +\n" +
                                " \"StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]");
                        predicates.add(a -> a.getOperationTypeString().equals(op));
                        break;
                    case "SequenceNumber":
                        long in = getLongUserInput("Valid Sequence Number: ");
                        predicates.add( a -> a.getSequenceNumber() == in);
                        break;
                    case "SegmentId":
                        in = getLongUserInput("Valid segmentId to search: ");
                        predicates.add( a -> a.getSegmentId() == in);
                        break;
                    case "Offset":
                        in = getLongUserInput("Valid offset to seach: ");
                        predicates.add( a -> a.getOffset() == in);
                        break;
                    case "Length":
                        in = getLongUserInput("Valid length to seach: ");
                        predicates.add( a -> a.getCacheLength() == in);
                        break;
                    case "Attributes":
                        in = getLongUserInput("Valid number of attributes to seach: ");
                        predicates.add( a -> a.getAttributes() == in );
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
