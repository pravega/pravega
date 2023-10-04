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
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import static io.pravega.cli.admin.utils.FileHelper.readAndWriteSegmentToFile;

public class ReadSegmentRangeCommand extends SegmentStoreCommand {
    /**
     * Creates a new instance of the ReadSegmentRangeCommand.
     *
     * @param args The arguments for the command.
     */
    public ReadSegmentRangeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(5);

        final String fullyQualifiedSegmentName = getArg(0);
        final long offset = getLongArg(1);
        final long length = getLongArg(2);
        final String segmentStoreHost = getArg(3);
        final String fileName = getArg(4);

        Preconditions.checkArgument(offset >= 0, "The provided offset cannot be negative.");
        Preconditions.checkArgument(length >= 0, "The provided length cannot be negative.");

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        ConnectionPool pool = createConnectionPool();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, pool);
        output("Downloading %d bytes from offset %d into %s.", length, offset, fileName);
        readAndWriteSegmentToFile(
            segmentHelper, segmentStoreHost, fullyQualifiedSegmentName, offset, length, fileName,
            getServiceConfig().getAdminGatewayPort(), super.authHelper.retrieveMasterToken());
        output("\nThe segment data has been successfully written into %s", fileName);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "read-segment", "Read a range from a given Segment into given file.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."),
                new ArgDescriptor("offset", "Starting point of the read request within the target Segment."),
                new ArgDescriptor("length", "Number of bytes to read."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new ArgDescriptor("file-name", "Name of the file to write the contents into."));
    }
}
