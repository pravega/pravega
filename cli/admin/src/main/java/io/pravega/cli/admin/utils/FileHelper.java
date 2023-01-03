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
package io.pravega.cli.admin.utils;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for file operations.
 */
public class FileHelper {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;
    private static final int READ_WRITE_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final String PROGRESS_BAR = "|/-\\";

    /**
     * Creates the file (and parent directory if required).
     *
     * @param fileName The name of the file to create.
     * @return A {@link File} object representing the filename provided.
     * @throws IOException if the file/directory already exists or if creation fails.
     */
    public static File createFileAndDirectory(String fileName) throws IOException {

        File f = new File(fileName);
        // If file exists throw FileAlreadyExistsException, an existing file should not be overwritten with new data.
        if (f.exists()) {
            throw new FileAlreadyExistsException("Cannot write segment data into a file that already exists.");
        }
        if (f.getParentFile() != null) {
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
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
     * @param adminGatewayPort          The Pravega Admin Gateway port.
     * @param authToken                 The delegation token.
     * @throws IOException if the file create/write fails.
     * @throws Exception if the request fails.
     */
    public static void readAndWriteSegmentToFile(SegmentHelper segmentHelper, String segmentStoreHost, String fullyQualifiedSegmentName,
                                          long offset, long length, String fileName, int adminGatewayPort, String authToken) throws IOException, Exception {
        File file = FileHelper.createFileAndDirectory(fileName);
        long currentOffset = offset;
        long bytesToRead = length;
        int progress = 0;
        @Cleanup
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        while (bytesToRead > 0) {
            long bufferLength = Math.min(READ_WRITE_BUFFER_SIZE, bytesToRead);
            CompletableFuture<WireCommands.SegmentRead> reply = segmentHelper.readSegment(fullyQualifiedSegmentName,
                    currentOffset, (int) bufferLength, new PravegaNodeUri(segmentStoreHost, adminGatewayPort), authToken);
            WireCommands.SegmentRead bufferRead = reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            int bytesRead = bufferRead.getData().readableBytes();
            // Write the buffer into the file.
            bufferRead.getData().readBytes(fileOutputStream, bytesRead);

            currentOffset += bytesRead;
            bytesToRead -= bytesRead;
            showProgress(progress++, String.format("Written %d/%d bytes.", length - bytesToRead, length));
        }
    }

    private static void showProgress(int progress, String message) {
        System.out.print("\r Processing " + PROGRESS_BAR.charAt(progress % PROGRESS_BAR.length()) + " : " + message);
    }
}
