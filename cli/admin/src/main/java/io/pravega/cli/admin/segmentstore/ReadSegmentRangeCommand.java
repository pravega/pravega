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
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReadSegmentRangeCommand extends SegmentStoreCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    /**
     * Creates a new instance of the ReadSegmentRangeCommand.
     *
     * @param args The arguments for the command.
     */
    public ReadSegmentRangeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException, TimeoutException {
        ensureArgCount(4);

        final String fullyQualifiedSegmentName = getArg(0);
        final int offset = getIntArg(1);
        final int length = getIntArg(2);
        final String segmentStoreHost = getArg(3);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        CompletableFuture<WireCommands.SegmentRead> reply = segmentHelper.readSegment(fullyQualifiedSegmentName,
                offset, length, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), "");
        WireCommands.SegmentRead segmentRead = reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("ReadSegment: %s", segmentRead.toString());
        output("SegmentRead content: %s", segmentRead.getData().toString(StandardCharsets.UTF_8));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "read-segment", "Read a range from a given Segment.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."),
                new ArgDescriptor("offset", "Starting point of the read request within the target Segment."),
                new ArgDescriptor("length", "Number of bytes to read."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
