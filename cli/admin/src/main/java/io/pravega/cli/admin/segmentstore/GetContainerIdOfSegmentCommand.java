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
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.SegmentToContainerMapper;
import com.google.common.base.Preconditions;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.CompletableFuture;

/**
 * Gets the container id of the given segment.
 * The arguments for the command that is the fully qualified segment name.
 */
public class GetContainerIdOfSegmentCommand extends SegmentStoreCommand {

    private final SegmentToContainerMapper segmentToContainerMapper;

    public GetContainerIdOfSegmentCommand(CommandArgs args) {
        super(args);
        int containerCount = getServiceConfig().getContainerCount();
        this.segmentToContainerMapper = new SegmentToContainerMapper(containerCount, true);
    }

    @Override
    public void execute() throws Exception {
        Preconditions.checkArgument(getArgCount() > 0 && getArgCount() <= 2, "Incorrect argument count.");
        final String fullyQualifiedSegmentName = getArg(0);
        String[] inputParam = fullyQualifiedSegmentName.split("/");
        Preconditions.checkArgument(inputParam.length == 3, "Invalid qualified-segment-name  '%s'", fullyQualifiedSegmentName);
        if (getArgCount() == 2) {
            final String check = getArg(1);
            Preconditions.checkArgument(check.equalsIgnoreCase("skipcheck"), "Command argument should either be skipcheck/SKIPCHECK or only existing segment name.");
            int containerId = segmentToContainerMapper.getContainerId(fullyQualifiedSegmentName);
            output("Container Id for the given Segment is:" + containerId);
        } else if (getArgCount() == 1) {
            try {
                String scope = inputParam[0];
                String stream = inputParam[1];
                String[] segmentArr = inputParam[2].split("\\.");
                @Cleanup
                CuratorFramework zkClient = createZKClient();
                @Cleanup
                ConnectionPool pool = createConnectionPool();
                @Cleanup
                SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, pool);
                int segmentNumber = Integer.parseInt(segmentArr[0]);
                int epoch = Integer.parseInt(segmentArr[2]);
                long segmentId = NameUtils.computeSegmentId(segmentNumber, epoch);
                CompletableFuture<WireCommands.StreamSegmentInfo> segmentInfoFuture = segmentHelper.getSegmentInfo(scope, stream, segmentId, super.authHelper.retrieveMasterToken(), 0L);
                segmentInfoFuture.join();
                int containerId = segmentToContainerMapper.getContainerId(fullyQualifiedSegmentName);
                output("Container Id for the given Segment is:" + containerId);
            }  catch (Exception ex) {
                 output("Error occurred while fetching containerId: %s", ex);
            }
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-container-id",
                                     "Get the Id of a Container that belongs to a segment.",
                                     new ArgDescriptor("qualified-segment-name",
                                                       "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."),
                                     new ArgDescriptor("optional:SKIPCHECK",
                                                       "If given as skipcheck/SKIPCHECK which is optional argument, it skips checking if the segment already exists."));
    }
}
