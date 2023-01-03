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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.stream.Collectors;

public class ControllerMetadataListKeysCommand extends ControllerMetadataCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataListKeysCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String tableName = getArg(0);
        final int keyCount = getIntArg(1);
        final String segmentStoreHost = getArg(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        HashTableIteratorItem<TableSegmentKey> keys = completeSafely(adminSegmentHelper.readTableKeys(tableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), keyCount,
                HashTableIteratorItem.State.EMPTY, super.authHelper.retrieveMasterToken(), 0L), tableName, null);
        if (keys == null) {
            return;
        }

        List<String> stringKeys = keys.getItems()
                .stream()
                .map(tableSegmentKey -> KEY_SERIALIZER.deserialize(getByteBuffer(tableSegmentKey.getKey())))
                .collect(Collectors.toList());
        output("List of at most %s keys in %s: ", keyCount, tableName);
        stringKeys.forEach(k -> output("- %s", k));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-keys", "List at most the required number of keys from the controller metadata table.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get the entry from. " +
                        "Run \"controller-metadata tables-info\" to get information about the controller metadata tables."),
                new ArgDescriptor("key-count", "The upper limit for the number of keys to be listed."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
