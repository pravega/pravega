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
package io.pravega.cli.admin.segmentstore.storage;

import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ListChunksCommand extends StorageCommand {

    public static final String LENGTH_IN_METADATA = "lengthInMetadata";
    public static final String LENGTH_IN_STORAGE = "lengthInStorage";
    public static final String START_OFFSET = "startOffset";
    public static final String EXISTS_IN_STORAGE = "existsInStorage";

    private static final Map<String, Function<WireCommands.ChunkInfo, Object>> CHUNK_INFO_FIELD_MAP =
            ImmutableMap.<String, Function<WireCommands.ChunkInfo, Object>>builder()
                    .put(LENGTH_IN_METADATA, WireCommands.ChunkInfo::getLengthInMetadata)
                    .put(LENGTH_IN_STORAGE, WireCommands.ChunkInfo::getLengthInStorage)
                    .put(START_OFFSET, WireCommands.ChunkInfo::getStartOffset)
                    .put(EXISTS_IN_STORAGE, WireCommands.ChunkInfo::isExistsInStorage)
                    .build();

    /**
     * Creates a new instance of the ListChunksCommand.
     *
     * @param args The arguments for the command.
     */
    public ListChunksCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);

        final String fullyQualifiedSegmentName = getArg(0);
        final String segmentStoreHost = getArg(1);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<WireCommands.StorageChunksListed> reply = adminSegmentHelper.listStorageChunks(fullyQualifiedSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());

        List<WireCommands.ChunkInfo> chunks = reply.join().getChunks();
        output("List of chunks for %s: ", fullyQualifiedSegmentName);
        chunks.forEach(chunk -> {
            output("- Chunk name = %s", chunk.getChunkName());
            CHUNK_INFO_FIELD_MAP.forEach((name, f) -> output("  %s = %s", name, f.apply(chunk)));
            output("");
        });
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-chunks", "Get the list of storage chunks for the given segment.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the segment."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
