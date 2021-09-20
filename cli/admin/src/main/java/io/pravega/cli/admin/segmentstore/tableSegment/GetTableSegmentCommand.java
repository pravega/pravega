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
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.pravega.cli.admin.utils.SerializerUtils.CHUNK_METADATA_FIELD_MAP;
import static io.pravega.cli.admin.utils.SerializerUtils.READ_INDEX_BLOCK_METADATA_FIELD_MAP;
import static io.pravega.cli.admin.utils.SerializerUtils.SEGMENT_METADATA_FIELD_MAP;
import static io.pravega.cli.admin.utils.SerializerUtils.parseStringData;

public class GetTableSegmentCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the GetTableSegmentCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableSegmentCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String key = getArg(1);
        final String segmentStoreHost = getArg(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<List<TableSegmentEntry>> reply = adminSegmentHelper.readTable(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(getCommandArgs().getState().getKeySerializer().serialize(key).array())),
                super.authHelper.retrieveMasterToken(), 0L);

        String value = getCommandArgs().getState().getValueSerializer().deserialize(ByteBuffer.wrap(reply.join().get(0).getValue().array()));
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
        if (dataMap.containsKey("key")) {
            if (CHUNK_METADATA_FIELD_MAP.keySet().stream().allMatch(dataMap::containsKey)) {
                output("SLTS metadata info (ChunkMetadata): ");
            } else if (SEGMENT_METADATA_FIELD_MAP.keySet().stream().allMatch(dataMap::containsKey)) {
                output("SLTS metadata info (SegmentMetadata): ");
            } else if (READ_INDEX_BLOCK_METADATA_FIELD_MAP.keySet().stream().allMatch(dataMap::containsKey)) {
                output("SLTS metadata info (ReadIndexBlockMetadata): ");
            }
        } else if (dataMap.containsKey("segmentId") && SEGMENT_METADATA_FIELD_MAP.keySet().stream().allMatch(dataMap::containsKey)) {
            output("Container metadata info: ");
        }
        dataMap.forEach((k, v) -> output("%s = %s;", k, v));
    }
}
