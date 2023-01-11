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
import io.pravega.cli.admin.segmentstore.storage.StorageCommand;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes a request to evictStorageReadIndexCache for specified Segment Store instance.
 */
public class EvictStorageReadIndexCacheCommand extends StorageCommand {
    private static final int REQUEST_TIMEOUT_SECONDS = 30;

    public EvictStorageReadIndexCacheCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {

        final String segmentStoreHost = getArg(0);
        final int containerId = getIntArg(1);
        final String fullyQualifiedSegmentName = getArg(2, null);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);

        if (null != fullyQualifiedSegmentName) {
            CompletableFuture<WireCommands.StorageReadIndexCacheEvictedForSegment> reply = adminSegmentHelper.evictStorageReadIndexCacheForSegment(containerId, fullyQualifiedSegmentName,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
            reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            output("Read Index Cache evicted for the Segment Container with containerId %d.", containerId);
        } else {
            CompletableFuture<WireCommands.StorageReadIndexCacheEvicted> reply = adminSegmentHelper.evictStorageReadIndexCache(containerId,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
            reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            output("Read Index Cache evicted for the Segment Container with containerId %d.", containerId);
        }
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "evict-read-index-cache", "Evict entire Read Index Cache.",
                new AdminCommand.ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new AdminCommand.ArgDescriptor("container-id", "The container Id of the Segment Container for which read index cache is evicted"),
                new AdminCommand.ArgDescriptor("fully-qualified-segment-name", "Fully qualified name of the Segment.", true));
    }
}
