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
package io.pravega.cli.admin.segmentstore.tableSegment;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ListTableSegmentKeysCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of ListTableSegmentKeysCommand.
     *
     * @param args The arguments for the command.
     */
    public ListTableSegmentKeysCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);
        ensureSerializersExist();

        final String fullyQualifiedTableSegmentName = getArg(0);
        final int keyCount = getIntArg(1);
        final String segmentStoreHost = getArg(2);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<HashTableIteratorItem<TableSegmentKey>> reply = adminSegmentHelper.readTableKeys(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), keyCount,
                HashTableIteratorItem.State.EMPTY, super.authHelper.retrieveMasterToken(), 0L);

        List<String> keys = reply.join().getItems()
                .stream()
                .map(tableSegmentKey -> getCommandArgs().getState().getKeySerializer().deserialize(getByteBuffer(tableSegmentKey.getKey())))
                .collect(Collectors.toList());
        output("List of at most %s keys in %s: ", keyCount, fullyQualifiedTableSegmentName);
        keys.forEach(k -> output("- %s", k));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-keys", "List at most the required number of keys from the table segment.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment."),
                new ArgDescriptor("key-count", "The upper limit for the number of keys to be listed."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
