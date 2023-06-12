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

import com.google.common.base.Charsets;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Executes a RemoveTableSegmentKey request against the chosen Segment Store instance.
 */
public class RemoveTableSegmentKeyCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of RemoveTableSegmentKeyCommand.
     *
     * @param args The arguments for the command.
     */
    public RemoveTableSegmentKeyCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);
        ensureSerializersExist();

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String key = getArg(1);
        final String segmentStoreHost = getArg(2);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<HashTableIteratorItem<TableSegmentKey>> getKeysReply = adminSegmentHelper.readTableKeys(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), Integer.MAX_VALUE,
                HashTableIteratorItem.State.EMPTY, super.authHelper.retrieveMasterToken(), 0L);

        TableSegmentKey keysList = getKeysReply.join().getItems()
                .stream()
                .filter(tableSegmentKey -> getCommandArgs().getState().getKeySerializer().deserialize(getByteBuffer(tableSegmentKey.getKey())).equals(key))
                .findFirst().orElse(null);

        if (keysList != null) {
            CompletableFuture<Void> reply = adminSegmentHelper.removeTableKeys(fullyQualifiedTableSegmentName, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                    Collections.singletonList(TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8))), super.authHelper.retrieveMasterToken(), 0L);
            reply.join();
            output("RemoveTableKey: %s removed successfully from %s", key, fullyQualifiedTableSegmentName);
        } else {
            output("RemoveTableKey failed: %s does not exist", key);
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "remove-key", "Removes table segment key. Use the command \"table-segment set-serializer <serializer-name>\" to use the appropriate serializer before using this command.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment."),
                new ArgDescriptor("key", "The key which is to be removed."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
