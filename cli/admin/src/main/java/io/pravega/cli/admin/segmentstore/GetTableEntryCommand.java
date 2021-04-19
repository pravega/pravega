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
package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GetTableEntryCommand extends SegmentStoreCommand {

    /**
     * Creates a new instance of the GetTableEntryCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableEntryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String fullyQualifiedTableName = getCommandArgs().getArgs().get(0);
        final TableSegmentKey key = TableSegmentKey.unversioned(getCommandArgs().getArgs().get(1).getBytes());
        final String segmentStoreHost = getCommandArgs().getArgs().get(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        CompletableFuture<List<TableSegmentEntry>> reply = segmentHelper.readTable(fullyQualifiedTableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), List.of(key), "", 0);
        try {
            output("GetTableEntryByKeyCommand: %s", reply.join()
                    .stream()
                    .filter(tableSegmentEntry -> tableSegmentEntry.getKey().equals(key))
                    .map(Object::toString)
                    .collect(Collectors.joining(", ")));
        } catch (Exception e) {
            output("Error executing GetTableEntryByKeyCommand command: %s", e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-table-entry", "Get a given Table Entry by Key.",
                new ArgDescriptor("qualified-table-name", "Fully qualified name of the Segment Table to get data from."),
                new ArgDescriptor("key", "Key of the Entry to fetch."),
                new ArgDescriptor("segmentstore-endpoint", "Name of the Segment Store we want to send this request."));
    }

}
