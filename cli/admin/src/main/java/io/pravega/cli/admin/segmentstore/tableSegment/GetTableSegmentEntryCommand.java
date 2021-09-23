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
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.pravega.cli.admin.serializers.AbstractSerializer.parseStringData;

public class GetTableSegmentEntryCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the GetTableSegmentEntryCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableSegmentEntryCommand(CommandArgs args) {
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

        ByteArraySegment serializedKey = new ByteArraySegment(getCommandArgs().getState().getKeySerializer().serialize(key));

        CompletableFuture<List<TableSegmentEntry>> reply = adminSegmentHelper.readTable(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(serializedKey.getCopy())),
                super.authHelper.retrieveMasterToken(), 0L);

        ByteBuffer serializedValue = getByteBuffer(reply.join().get(0).getValue());
        String value = getCommandArgs().getState().getValueSerializer().deserialize(serializedValue);
        output("For the given key: %s", key);
        userFriendlyOutput(value);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get", "Get the value for the given key in the table.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get info from."),
                new ArgDescriptor("key", "The key to be queried."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }

    private void userFriendlyOutput(String data) {
        Map<String, String> dataMap = parseStringData(data);
        output("%s metadata info: ", getCommandArgs().getState().getValueSerializer().getName());
        dataMap.forEach((k, v) -> output("%s = %s;", k, v));
    }
}
