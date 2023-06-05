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

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;

/**
 * Executes a DeleteSegment request against the chosen Segment Store instance.
 */
public class DeleteSegmentCommand extends SegmentStoreCommand {
    /**
     * Creates a new instance of the DeleteSegmentCommand.
     *
     * @param args The arguments for the command.
     */
    public DeleteSegmentCommand(CommandArgs args) {
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
        ConnectionPool pool = createConnectionPool();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, pool);

        String[] inputParam = fullyQualifiedSegmentName.split("/");
        Preconditions.checkArgument(inputParam.length == 3, "Invalid qualified-segment-name  '%s'", fullyQualifiedSegmentName);

        try {
            CompletableFuture<WireCommands.StreamSegmentInfo> segmentInfoFuture = segmentHelper.getSegmentInfo(fullyQualifiedSegmentName,
                    new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken(), 0L);
            segmentInfoFuture.join();
            CompletableFuture<Void> reply = segmentHelper.deleteSegment(fullyQualifiedSegmentName,
                        super.authHelper.retrieveMasterToken(),
                        new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), 0L);
            reply.join();
            output("DeleteSegment: %s deleted successfully", fullyQualifiedSegmentName);
        } catch (Exception ex) {
            output("DeleteSegment failed for %s - %s", fullyQualifiedSegmentName ,ex.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "delete-segment", "Deletes a given Segment.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to delete (e.g., scope/stream/0.#epoch.0)."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}