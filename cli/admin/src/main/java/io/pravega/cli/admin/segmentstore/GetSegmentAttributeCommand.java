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
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class GetSegmentAttributeCommand extends SegmentStoreCommand {

    /**
     * Creates a new instance of the GetSegmentInfoCommand.
     *
     * @param args The arguments for the command.
     */
    public GetSegmentAttributeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String fullyQualifiedSegmentName = getArg(0);
        final UUID attributeId = getUUIDArg(1);
        final String segmentStoreHost = getArg(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        ConnectionPool pool = createConnectionPool();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, pool);
        CompletableFuture<WireCommands.SegmentAttribute> reply = segmentHelper.getSegmentAttribute(fullyQualifiedSegmentName,
                attributeId, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
        output("GetSegmentAttribute: %s", reply.join().toString());
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-segment-attribute", "Gets an attribute for a Segment.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."),
                new ArgDescriptor("attribute-id", "UUID of the Segment Attribute to fetch."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
