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
package io.pravega.cli.admin.controller.metadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.CommandArgs;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.DELETED_STREAMS_TABLE;
import static io.pravega.shared.NameUtils.EPOCHS_WITH_TRANSACTIONS_TABLE;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.METADATA_TABLE;
import static io.pravega.shared.NameUtils.TRANSACTIONS_IN_EPOCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.WRITERS_POSITIONS_TABLE;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

public class ControllerMetadataTablesInfoCommand extends ControllerMetadataCommand {

    private static final String TEST_SCOPE = "testScope";
    private static final String TEST_STREAM = "testStream";
    private static final String PLACEHOLDER_SCOPE = "<scope-name>";
    private static final String PLACEHOLDER_STREAM = "<stream-name>";
    private static final String PLACEHOLDER_UUID = "<stream-uuid>";
    private static final String PLACEHOLDER_EPOCH = "<stream-epoch>";

    private static final Map<String, List<String>> TABLE_NAMES =
            ImmutableMap.<String, List<String>>builder()
                    .put(String.format(METADATA_TABLE, "<id>"),
                            ImmutableList.of(
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, PLACEHOLDER_SCOPE, PLACEHOLDER_STREAM,
                                            String.format(METADATA_TABLE, PLACEHOLDER_UUID)),
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM, String.format(METADATA_TABLE, UUID.randomUUID())),
                                    "Stores stream properties like state, current epoch number, creation time, etc."))
                    .put(String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, "<id>"),
                            ImmutableList.of(
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, PLACEHOLDER_SCOPE, PLACEHOLDER_STREAM,
                                            String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, PLACEHOLDER_UUID)),
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                                            String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, UUID.randomUUID())),
                                    "Stores the epoch numbers for transactions."))
                    .put(String.format(WRITERS_POSITIONS_TABLE, "<id>"),
                            ImmutableList.of(
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, PLACEHOLDER_SCOPE, PLACEHOLDER_STREAM,
                                            String.format(WRITERS_POSITIONS_TABLE, PLACEHOLDER_UUID)),
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                                            String.format(WRITERS_POSITIONS_TABLE, UUID.randomUUID())),
                                    "Stores the writers' mark positions during watermarking."))
                    .put(String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, "<epoch>", "<id>"),
                            ImmutableList.of(
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, PLACEHOLDER_SCOPE, PLACEHOLDER_STREAM,
                                            String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, PLACEHOLDER_EPOCH, PLACEHOLDER_UUID)),
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                                            String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, 1, UUID.randomUUID())),
                                    "Stores the set of active transactions in an epoch."))
                    .put("completedTransactionsBatches",
                            ImmutableList.of(
                                    COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                    COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                    "Stores the completed transactions' batch numbers."))
                    .put(String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, "<batch>"),
                            ImmutableList.of(
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, "<batch>")),
                                    getQualifiedTableName(INTERNAL_SCOPE_NAME, String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, 2)),
                                    "Stores the set of completed transactions."))
                    .put("deletedStreams",
                            ImmutableList.of(
                                    DELETED_STREAMS_TABLE,
                                    DELETED_STREAMS_TABLE,
                                    "Stores the set of deleted streams and their last segment number."))
                    .build();

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataTablesInfoCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        Preconditions.checkArgument(getCommandArgs().getArgs().size() == 0, "Not expecting any arguments.");
        TABLE_NAMES.forEach((name, info) -> printTableInfo(name, info.get(0), info.get(2), info.get(1)));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "tables-info", "List all the controller metadata tables.");
    }

    private void printTableInfo(String tableName, String tableFormat, String tableInfo, String tableExample) {
        output(String.format("%s --> Table name format: '%s'.", tableName, tableFormat));
        output(tableInfo);
        output("eg: %s\n", tableExample);
    }
}
