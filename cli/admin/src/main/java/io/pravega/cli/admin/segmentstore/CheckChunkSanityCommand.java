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
import io.pravega.cli.admin.segmentstore.storage.StorageCommand;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes a request to checkChunkSanity for the chosen Segment Store instance.
 */
public class CheckChunkSanityCommand extends StorageCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 30;

    /**
     * Creates new instance of the CheckChunkSanityCommand.
     * @param args The arguments for the command.
     */
    public CheckChunkSanityCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);

        final String segmentStoreHost = getArg(0);
        final int containerId = getIntArg(1);
        final String fullyQualifiedChunkName = getArg(2) + System.currentTimeMillis();
        final int dataSize = getIntArg(3);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);

        CompletableFuture<WireCommands.ChunkSanityChecked> reply = adminSegmentHelper.checkChunkSanity(containerId, fullyQualifiedChunkName, dataSize,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
        reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("Chunk sanity checked for the Segment Container with containerId %d.", containerId);

    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "check-chunk-sanity", "Check sanity of the given chunk with range of operations performed on it.",
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new ArgDescriptor("container-id", "The container Id of the Segment Container for which sanity operation is performed."),
                new ArgDescriptor("qualified-chunk-name", "Fully qualified name of the Chunk to perform sanity operations like (e.g., create, check if exists, write, read, delete)."),
                new ArgDescriptor("data-size", "Data size of the bytes to be read."));
    }
}
