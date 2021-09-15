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
package io.pravega.cli.admin.segmentstore.tableSegment;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.CompletableFuture;

public class GetTableSegmentInfoCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the GetTableSegmentInfoCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableSegmentInfoCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String segmentStoreHost = getArg(1);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<WireCommands.TableSegmentInfo> reply = adminSegmentHelper.getTableSegmentInfo(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());

        output("GetTableSegmentInfo: %s", reply.join().toString());
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-info", "Get the details of a given table segment",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get info from."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
