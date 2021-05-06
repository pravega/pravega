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

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlushToStorageCommand extends SegmentStoreCommand {
    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    /**
     * Creates a new instance of the FlushToStorageCommand.
     *
     * @param args The arguments for the command.
     */

    public FlushToStorageCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException, TimeoutException {
        final String segmentStoreHost = getArg(0);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        CompletableFuture<WireCommands.FlushedStorage> reply = segmentHelper.flushToStorage(
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), "");
        reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("Flushed durable log to the storage.");
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "flushToStorage", "Flushes durable log to the storage.");
    }
}
