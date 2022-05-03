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
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EvictReadIndexCacheCommand extends SegmentStoreCommand{
    private static final int REQUEST_TIMEOUT_SECONDS = 30;

    public EvictReadIndexCacheCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {



        final int containerId = getIntArg(0);
        final String segmentStoreHost = getArg(1);
        final String fullyQualifiedSegmentName = getArg(2);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);

        if(null != fullyQualifiedSegmentName) {
            CompletableFuture<WireCommands.ReadIndexCacheEvictedForSegment> reply = adminSegmentHelper.evictReadIndexCacheForSegment(containerId, fullyQualifiedSegmentName,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
            reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            output("Meta Data Cache evicted for the Segment Container with containerId %d.", containerId);
        }
        else {
            CompletableFuture<WireCommands.ReadIndexCacheEvicted> reply = adminSegmentHelper.evictReadIndexCache(containerId,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
            reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            output("Meta Data Cache evicted for the Segment Container with containerId %d.", containerId);
        }
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "evict-meta-data-cache", "Persist the given Segment Container into Storage.",
                new AdminCommand.ArgDescriptor("container-id", "The container Id of the Segment Container that needs to be persisted, " +
                        "if given as \"all\" all the containers will be persisted."),
                new AdminCommand.ArgDescriptor("segmentStore-endpoint", "Address of the Segment Store we want to send this request."),
                new AdminCommand.ArgDescriptor("fully-qualified-segment-name", "Fully qualified name of the Segment for which .", true));
    }
}
