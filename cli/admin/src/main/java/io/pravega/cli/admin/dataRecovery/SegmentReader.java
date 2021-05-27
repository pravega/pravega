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
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Reads the content's of a given segment.
 */
public class SegmentReader extends DataRecoveryCommand {

    private static final Duration TIMEOUT = Duration.ofMillis(240 * 1000);
    private static final int CONTAINER_EPOCH = 1;
    private static final int BUFFER_SIZE = 1 * 1024 * 1024;
    private final ScheduledExecutorService scheduledExecutorService = getCommandArgs().getState().getExecutor();
    private final StorageFactory storageFactory;
    private FileOutputStream fileOutputStream;
    private String filePath;
    private String segmentName;

    /**
     * Creates an instance of SegmentReader class.
     *
     * @param args The arguments for the command.
     */
    public SegmentReader(CommandArgs args) {
        super(args);
        this.storageFactory = createStorageFactory(this.scheduledExecutorService);
    }

    /**
     * Creates a file for writing segment's contents.
     *
     * @throws Exception   When failed to create/delete file(s).
     */
    private void createFileAndDirectory() throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        // Set up directory
        this.filePath = System.getProperty("user.dir") + Path.SEPARATOR + descriptor().getName() + "_" + fileSuffix;

        // If path given as command args, use it
        this.segmentName = getCommandArgs().getArgs().get(0);
        if (getArgCount() >= 2) {
            this.filePath = getCommandArgs().getArgs().get(1);
            if (this.filePath.endsWith(Path.SEPARATOR)) {
                this.filePath = this.filePath.substring(0, this.filePath.length()-1);
            }
        }

        // Create a directory for storing the file.
        File dir = new File(this.filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        File f = new File(this.filePath + Path.SEPARATOR + "segmentContent" + ".txt");
        if (!f.createNewFile()) {
            outputError("Failed to create file '%s'.", f.getAbsolutePath());
            throw new Exception("Failed to create file " + f.getAbsolutePath());
        }
        this.fileOutputStream = new FileOutputStream(f.getAbsolutePath(), true);
        outputInfo("Created file '%s'", f.getAbsolutePath());
    }

    @Override
    public void execute() throws Exception {
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();

        // Get the storage using the config.
        storage.initialize(CONTAINER_EPOCH);
        outputInfo("Loaded %s Storage.", getServiceConfig().getStorageImplementation().toString());

        // create CSV files for listing the segments and their details
        createFileAndDirectory();

        outputInfo("Writing segment's content to the file...");
        int countEvents = readSegment(storage, this.segmentName, TIMEOUT);
        outputInfo("Number of events found = %d.", countEvents);

        outputInfo("Closing the file...");
        this.fileOutputStream.close();

        outputInfo("The segment's contents have been written to the file.");
        outputInfo("Path to the file '%s'", this.filePath);
        outputInfo("Done!");
    }

    /**
     * Reads the contents of a given segment.
     * @param storage                   A storage instance to read the segment's contents.
     * @param segmentName               The name of the segment.
     * @param timeout                   A timeout for the operation.
     * @throws Exception                If an exception occurred.
     * @return                          Number of events found.
     */
    private int readSegment(Storage storage, String segmentName, Duration timeout)
            throws Exception {

        val segmentInfo = storage.getStreamSegmentInfo(segmentName, timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        int bytesToRead = (int) segmentInfo.getLength();
        val sourceHandle = storage.openRead(segmentName).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        byte[] header; // stores header
        byte[] data; // stores data

        int countEvents = 0; // Keeps count of events
        int offset = 0;     // Moves the offset to read a new chunk less then equal to BUFFER_SIZE

        byte[] buffer = new byte[BUFFER_SIZE];
        int runnerOffset;
        while (offset < bytesToRead) { // read till offset reaches the end
            int size = storage.read(sourceHandle, offset, buffer, 0, Math.min(BUFFER_SIZE, bytesToRead - offset), timeout)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            outputInfo("Read size: %d", size);

            runnerOffset = 0; // to read the current buffer
            while ( (runnerOffset + 8) < size) { // It should be less, unless there can be 0 bytes event
                header = Arrays.copyOfRange(buffer, runnerOffset, runnerOffset + 8);

                long length = convertByteArrayToLong(header);
                if (runnerOffset + 8 + length > size) { // if data range exceeds the buffer size, break
                    break;
                }
                if (length != 0) { // If there is something to read
                    data = Arrays.copyOfRange(buffer, runnerOffset + 8, runnerOffset + 8 + (int) length);
                    this.fileOutputStream.write(data, 0, (int) length);
                    this.fileOutputStream.write("\n".getBytes());
                    countEvents++;
                }
                runnerOffset += length + 8;
            }

            offset += runnerOffset;
        }
        return countEvents;
    }

    private long convertByteArrayToLong(byte[] longBytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(longBytes);
        return byteBuffer.getLong();
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "read-segment", "Writes the contents of the segment to a file.");
    }
}
