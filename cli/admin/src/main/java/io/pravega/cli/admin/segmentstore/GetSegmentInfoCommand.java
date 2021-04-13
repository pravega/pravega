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
import io.pravega.common.cluster.Host;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;

/**
 * Executes a GetSegmentInfo request against the chosen Segment Store instance.
 */
public class GetSegmentInfoCommand extends SegmentStoreCommand {

    /**
     * Creates a new instance of the GetSegmentInfoCommand.
     *
     * @param args The arguments for the command.
     */
    public GetSegmentInfoCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(4);

        final String scopeName = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        final int segmentId = getIntArg(2);
        final String segmentStoreHost = getCommandArgs().getArgs().get(3);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, true, new Host(segmentStoreHost, getServiceConfig().getAdminGatewayPort(), ""));
        CompletableFuture<WireCommands.StreamSegmentInfo> reply = segmentHelper.getSegmentInfo(scopeName, streamName, segmentId, "");
        try {
            output("StreamSegmentInfo: %s", reply.join().toString());
        } catch (Exception e) {
            output("Error executing GetSegmentInfoCommand command: %s", e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-segment-info", "Get the details of a given Segment.",
                new ArgDescriptor("scope-name", "Name of the Scope where the Segment belongs to."),
                new ArgDescriptor("stream-name", "Name of the Stream where the Segment belongs to."),
                new ArgDescriptor("segment-id", "Id of the Segment to get information from."),
                new ArgDescriptor("segmentstore-host", "Host for the Segment Store storing this Segment."));
    }
}
