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
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isStreamMetadataTableName;

public class ControllerMetadataListEntriesCommand extends ControllerMetadataCommand {
    
    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataListEntriesCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String tableName = getArg(0);
        final int entryCount = getIntArg(1);
        final String segmentStoreHost = getArg(2);
        Preconditions.checkArgument(!isStreamMetadataTableName(tableName), "The given table %s is a stream metadata table. " +
                "Stream metadata tables are unsupported by this command.", tableName);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        HashTableIteratorItem<TableSegmentEntry> entries  = completeSafely(adminSegmentHelper.readTableEntries(tableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), entryCount,
                HashTableIteratorItem.State.EMPTY, super.authHelper.retrieveMasterToken(), 0L), tableName, null);
        if (entries == null) {
            return;
        }

        Map<String, List<String>> stringEntriesMap = entries.getItems().stream()
                .collect(Collectors.toMap(entry -> KEY_SERIALIZER.deserialize(getByteBuffer(entry.getKey().getKey())), entry -> {
                    ControllerMetadataSerializer serializer =
                            new ControllerMetadataSerializer(tableName, KEY_SERIALIZER.deserialize(getByteBuffer(entry.getKey().getKey())));
                    return List.of(serializer.deserialize(getByteBuffer(entry.getValue())).toString(), serializer.getMetadataType());
                }));

        output("List of at most %s entries in %s: ", entryCount, tableName);
        stringEntriesMap.forEach((key, value) -> {
            output("- %s", key);
            userFriendlyOutput(value.get(0), value.get(1));
            output("");
        });
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-entries", "List at most the required number of entries from the controller metadata table. " +
                "Unsupported for stream metadata tables.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get the entry from. " +
                        "Run \"controller-metadata tables-info\" to get information about the controller metadata tables."),
                new ArgDescriptor("entry-count", "The upper limit for the number of entries to be listed."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
