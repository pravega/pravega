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

import static java.lang.Integer.parseInt;

/**
 * Executes a FlushToStorage request against the chosen Segment Store instance.
 */
public class FlushToStorageCommand extends ContainerCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 30;
    private static final String ALL_CONTAINERS = "all";

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

        final String containerId = getArg(0);
        final String segmentStoreHost = getArg(1);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        if (containerId.equalsIgnoreCase(ALL_CONTAINERS)) {
            int containerCount = getServiceConfig().getContainerCount();
            for (int id = 0; id < containerCount; id++) {
                flushContainerToStorage(adminSegmentHelper, id, segmentStoreHost);
            }
        } else {
            flushContainerToStorage(adminSegmentHelper, parseInt(containerId), segmentStoreHost);
        }
    }

    private void flushContainerToStorage(AdminSegmentHelper adminSegmentHelper, int containerId, String segmentStoreHost) throws Exception {
        CompletableFuture<WireCommands.StorageFlushed> reply = adminSegmentHelper.flushToStorage(containerId,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
        reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("Flushed the Segment Container with containerId %d to Storage.", containerId);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "flush-to-storage", "Persist the given Segment Container into Storage.",
                new ArgDescriptor("container-id", "The container Id of the Segment Container that needs to be persisted, " +
                        "if given as \"all\" all the containers will be persisted."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
