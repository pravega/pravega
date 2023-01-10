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
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;

/**
 * This command provides an administrator with the basic primitives to inspect a DurableLog.
 * The workflow of this command is as follows:
 * 1. Reads the original DurableLog
 * 2. User input for conditions to inspect.
 * 3. Lists the result and is saved in given filename
 */
public class DurableLogInspectCommand extends DurableDataLogRepairCommand {

    private static final String SEQUENCE_NUMBER = "SequenceNumber";
    private static final String SEGMENT_ID = "SegmentId";
    private static final String OFFSET = "Offset";
    private static final String LENGTH = "Length";
    private static final String ATTRIBUTES = "Attributes";
    private static final String LESS_THAN = "<";
    private static final String GREATER_THAN = ">";
    private static final String LESS_THAN_EQUAL_TO = "<=";
    private static final String GREATER_THAN_EQUAL_TO = ">=";
    private static final String NOT_EQUAL_TO = "!=";
    private static final String OPERATOR_AND = "and";
    private static final String NEW_LINE_SEPARATOR = "\n";

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
        ensureArgCount(2);
        int containerId = getIntArg(0);
        final String fileName = getArg(1);
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

        // Print the operations for the selected durableLog.
        int durableLogReadOperations = readDurableDataLog(containerId, originalDataLog);

        output("Total operations read from original log :" + durableLogReadOperations);
        output("\n Note: Previous result files if present will be deleted \n");

        FileHelpers.deleteFileOrDirectory(new File(fileName));
        // Get user input.
        Predicate<OperationInspectInfo> durableLogPredicates = getConditionTypeFromUser();

        durableLogReadOperations = filterResult(durableLogPredicates, containerId, originalDataLog, fileName);
        //List the number of operations matched the user condition.
        output("Total operations read matching conditions: " + durableLogReadOperations);
        output("Process completed successfully!!");
    }

    protected int readDurableDataLog(int containerId, DebugDurableDataLogWrapper originalDataLog) throws Exception {

        int operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback((a, b) ->  {
            output("Reading: " + a);
            }, containerId, originalDataLog.asReadOnly());
        return operationsReadFromOriginalLog;
    }

    @VisibleForTesting
    static OperationInspectInfo getActualOperation(Operation op) {
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

    private int filterResult(Predicate<OperationInspectInfo> predicate, int containerId, DebugDurableDataLogWrapper originalDataLog, String fileName) throws Exception {
        AtomicInteger res = new AtomicInteger();

        @Cleanup
        FileWriter writer = new FileWriter(createFileAndDirectory(fileName));

        readDurableDataLogWithCustomCallback((a, b) -> {
                    if (predicate.test(getActualOperation(a))) {
                        output(a.toString());
                        try {
                            writer.write(getActualOperation(a).toString() + NEW_LINE_SEPARATOR);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        res.getAndIncrement();
                    }
                }, containerId, originalDataLog.asReadOnly());
        writer.flush();
        writer.close();
        return res.get();
    }

    /**
     * Guides the users to a set of options for creating predicates for printing
     * operations of durable Log .
     *
     * @return List of predicates.
     */
    @VisibleForTesting
    Predicate<OperationInspectInfo> getConditionTypeFromUser() {
        Predicate<OperationInspectInfo> predicate = null;
        List<Predicate<OperationInspectInfo>> predicates = new ArrayList<>();
        boolean finishInputCommands = false;
        boolean next = false;
        String clause = OPERATOR_AND;
        while (!finishInputCommands) {
            boolean showMessage = true;
            try {
                if (next) {
                    clause = getStringUserInput("Select conditional operator: [and/or]");
                }
                final String conditionTpe = getStringUserInput("Select condition type to display the output: [OperationType/SequenceNumber/SegmentId/Offset/Length/Attributes]");
                switch (conditionTpe) {
                    case "OperationType":
                        String op = getStringUserInput("Enter valid operation type: [DeleteSegmentOperation|MergeSegmentOperation|MetadataCheckpointOperation|\" +\n" +
                                " \"StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|\" +\n" +
                                " \"StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]");
                        predicates.add(a -> a.getOperationTypeString().equals(op));
                        break;
                    case SEQUENCE_NUMBER:
                    case SEGMENT_ID:
                    case OFFSET:
                    case LENGTH:
                    case ATTRIBUTES:
                        op = getStringUserInput("Search operation based on: [value/range]");
                        if (op.equals("value")) {
                            long in = getLongUserInput("Enter valid " + conditionTpe);
                            predicates.add(a -> a.getSequenceNumber() == in);
                        } else {
                            predicates.add(valueOrRangeInput(conditionTpe));
                        }
                        break;
                    default:
                        showMessage = false;
                        output("Invalid operation, please select one of [OperationType/SequenceNumber/SegmentId/Offset/Length/Attributes]");
                }
                predicate = clause.equals(OPERATOR_AND) ?
                        predicates.stream().reduce(Predicate::and).orElse(x -> true) : predicates.stream().reduce(Predicate::or).orElse(x -> false);
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                outputException(ex);
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
            if (showMessage) {
                output("You can continue adding conditions for inspect.");
            }
            finishInputCommands = !confirmContinue();
            next = !finishInputCommands;
        }
        output("Value of predicates is : " + predicates);
        return predicate;
    }

    /**
     * Guides the users to a set of options for creating range for selected
     * option by the user.
     *
     * @return Predicate<OperationInspectInfo>.
     */
    private Predicate<OperationInspectInfo> valueOrRangeInput(String conditionTpe) {
        List<Predicate<OperationInspectInfo>> predicates = new ArrayList<>();
        Predicate<OperationInspectInfo> predicate = null;
        String clause = OPERATOR_AND;
        boolean finish = false, next = false;
        while (!finish) {
            if (next) {
                clause = getStringUserInput("Select conditional operator: [and/or]");
            }
            final String input = getStringUserInput("Select operator : [</>/<=/>=/!=]");
            final long in = getLongUserInput("Enter range " + conditionTpe);
            switch (input) {
                case LESS_THAN:
                    predicates.add(a -> a.getSequenceNumber() < in);
                    break;
                case GREATER_THAN:
                    predicates.add(a -> a.getSequenceNumber() > in);
                    break;
                case LESS_THAN_EQUAL_TO:
                    predicates.add(a -> a.getSequenceNumber() <= in);
                    break;
                case GREATER_THAN_EQUAL_TO:
                    predicates.add(a -> a.getSequenceNumber() >= in);
                    break;
                case NOT_EQUAL_TO:
                    predicates.add(a -> a.getSequenceNumber() != in);
                    break;
                default:
                    output("Invalid input please select valid operator : [</>/<=/>=/!=]");
            }
            predicate = clause.equals(OPERATOR_AND) ?
                    predicates.stream().reduce(Predicate::and).orElse(x -> true) : predicates.stream().reduce(Predicate::or).orElse(x -> false);
            output("You can continue adding range operators for inspect.");
            finish = !confirmContinue();
            next = !finish;
        }
        return predicate;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-inspect", "Allows to inspect DurableLog " +
                "damaged/corrupted Operations.",
                new ArgDescriptor("container-id", "Id of the Container to inspect."),
                new ArgDescriptor("filename", "Name of the file to save the result."));
    }

    @Getter
    @Setter
    static class OperationInspectInfo extends Operation {
        public static final long DEFAULT_ABSENT_VALUE = Long.MIN_VALUE + 1;
        private final String operationTypeString;
        private final long length;
        private final long segmentId;
        private final long offset;
        private final long attributes;

        public OperationInspectInfo(long sequenceNumber, String operationTypeString, long length, long segmentId, long offset, long attributes) {
            super();
            setSequenceNumber(sequenceNumber);
            this.operationTypeString = operationTypeString;
            this.length = length == DEFAULT_ABSENT_VALUE ? 0 : length;
            this.segmentId = segmentId == DEFAULT_ABSENT_VALUE ? 0 : segmentId;
            this.offset = offset == DEFAULT_ABSENT_VALUE ? 0 : offset;
            this.attributes = attributes == DEFAULT_ABSENT_VALUE ? 0 : attributes;
        }

        @Override
        public String toString() {
            return "{ operationTypeString=" + operationTypeString +
                    ", sequenceNumber=" + this.getSequenceNumber() +
                    ", length=" + length +
                    ", segmentId=" + segmentId +
                    ", offset=" + offset +
                    ", attributes=" + attributes +
                    " }";
        }
    }

}
