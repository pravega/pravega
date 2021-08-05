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
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes a FlushToStorage request against the chosen Segment Store instance.
 */
public class FlushToStorageCommand extends ContainerCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    /**
     * Creates new instance of the FlushToStorageCommand.
     *
     * @param args The arguments for the command.
     */
    public FlushToStorageCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);

        final int containerId = getIntArg(0);
        final String segmentStoreHost = getArg(1);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = (AdminSegmentHelper) instantiateSegmentHelper(zkClient);
        CompletableFuture<WireCommands.StorageFlush> reply = adminSegmentHelper.flushToStorage(containerId,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
        reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("Flushed the Segment Container with containerId %d to Storage successfully.", containerId);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "flush-to-storage", "Persist the given Segment Container into Storage.",
                new ArgDescriptor("container-id", "The container Id of the Segment Container that needs to be persisted."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
