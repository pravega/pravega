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
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReadSegmentRangeCommand extends SegmentStoreCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;
    private static final int READ_WRITE_BUFFER_SIZE = 2 * 1024 * 1024;

    /**
     * Creates a new instance of the ReadSegmentRangeCommand.
     *
     * @param args The arguments for the command.
     */
    public ReadSegmentRangeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException, TimeoutException, IOException {
        ensureArgCount(5);

        final String fullyQualifiedSegmentName = getArg(0);
        final int offset = getIntArg(1);
        final int length = getIntArg(2);
        final String segmentStoreHost = getArg(3);
        final String fileName = getArg(4);

        Preconditions.checkArgument(offset >= 0, "The provided offset cannot be negative.");
        Preconditions.checkArgument(length >= 0, "The provided length cannot be negative.");

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        readAndWriteSegmentToFile(segmentHelper, segmentStoreHost, fullyQualifiedSegmentName, offset, length, fileName);
        output("\nThe segment data has been successfully written into %s", fileName);
    }

    /**
     * Creates the file (and parent directory if required) into which thee segment data is written.
     *
     * @param fileName
     * @return A {@link File} object representing the filename provided.
     * @throws FileAlreadyExistsException if the file already exists, to avoid any accidental overwrites.
     * @throws IOException if the file/directory creation fails.
     */
    private File createFileAndDirectory(String fileName) throws IOException {
        File f = new File(fileName);
        // If file exists throw FileAlreadyExistsException, an existing file should not be overwritten with new data.
        if (f.exists()) {
            throw new FileAlreadyExistsException("Cannot write segment data into a file that already exists.");
        }
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        return f;
    }

    /**
     * Reads the contents of the segment starting from the given offset and writes into the provided file.
     *
     * @param segmentHelper             A {@link SegmentHelper} instance to read the segment.
     * @param segmentStoreHost          Address of the segment-store to read from.
     * @param fullyQualifiedSegmentName The name of the segment.
     * @param offset                    The starting point from where the segment is to be read.
     * @param length                    The number of bytes to read.
     * @param fileName                  A name of the file to which the data will be written.
     * @throws IOException if the file create/write fails.
     * @throws InterruptedException if the request fails.
     * @throws ExecutionException if the request fails.
     * @throws TimeoutException if the request fails.
     */
    private void readAndWriteSegmentToFile(SegmentHelper segmentHelper, String segmentStoreHost, String fullyQualifiedSegmentName,
                                           int offset, int length, String fileName) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        File file = createFileAndDirectory(fileName);

        output("Downloading %d bytes from offset %d into %s.", length, offset, fileName);
        int currentOffset = offset;
        int bytesToRead = length;
        int progress = 0;

        while (bytesToRead > 0) {
            int bufferLength = Math.min(READ_WRITE_BUFFER_SIZE, bytesToRead);
            CompletableFuture<WireCommands.SegmentRead> reply = segmentHelper.readSegment(fullyQualifiedSegmentName,
                    currentOffset, bufferLength, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
            WireCommands.SegmentRead bufferRead = reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // Write the buffer into the file.
            try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
                bufferRead.getData().readBytes(fileOutputStream, bufferLength);
            }

            currentOffset += bufferLength;
            bytesToRead -= bufferLength;
            showProgress(progress++, String.format("Written %d/%d bytes.", length - bytesToRead, length));
        }
    }

    private void showProgress(int progress, String message) {
        String status = "|/-\\";
        System.out.print("\r Processing " + status.charAt(progress % status.length()) + " : " + message);
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
