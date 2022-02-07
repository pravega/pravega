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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.Cleanup;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Lists all non-shadow segments from there from the storage. The storage is loaded using the config properties.
 */
public class StorageListSegmentsCommand extends DataRecoveryCommand {
    /**
     * Header line for writing segments' details to csv files.
     */
    private static final List<String> HEADER = Arrays.asList("Sealed Status", "Length", "Segment Name");
    private static final int CONTAINER_EPOCH = 1;
    private final int containerCount;
    private final ScheduledExecutorService scheduledExecutorService = getCommandArgs().getState().getExecutor();
    private final SegmentToContainerMapper segToConMapper;
    private final StorageFactory storageFactory;
    private final FileWriter[] csvWriters;
    private String filePath;

    /**
     * Creates an instance of StorageListSegmentsCommand class.
     *
     * @param args The arguments for the command.
     */
    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.segToConMapper = new SegmentToContainerMapper(this.containerCount, true);
        this.storageFactory = createStorageFactory(this.scheduledExecutorService);
        this.csvWriters = new FileWriter[this.containerCount];
    }

    /**
     * Creates a csv file for each container. All segments belonging to a containerId have their details written to the
     * csv file for that container.
     *
     * @throws Exception   When failed to create/delete file(s).
     */
    private void createCSVFiles() throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        // Set up directory for storing csv files
        this.filePath = System.getProperty("user.dir") + Path.SEPARATOR + descriptor().getName() + "_" + fileSuffix;

        // If path given as command args, use it
        if (getArgCount() >= 1) {
            this.filePath = getCommandArgs().getArgs().get(0);
            if (this.filePath.endsWith(Path.SEPARATOR)) {
                this.filePath = this.filePath.substring(0, this.filePath.length()-1);
            }
        }

        // Create a directory for storing files.
        File dir = new File(this.filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        for (int containerId = 0; containerId < this.containerCount; containerId++) {
            File f = new File(this.filePath + Path.SEPARATOR + "Container_" + containerId + ".csv");
            if (f.exists()) {
                output("File '%s' already exists.", f.getAbsolutePath());
                if (!f.delete()) {
                    outputError("Failed to delete the file '%s'.", f.getAbsolutePath());
                    throw new Exception("Failed to delete the file " + f.getAbsolutePath());
                }
            }
            if (!f.createNewFile()) {
                outputError("Failed to create file '%s'.", f.getAbsolutePath());
                throw new Exception("Failed to create file " + f.getAbsolutePath());
            }
            this.csvWriters[containerId] = new FileWriter(f.getAbsolutePath());
            output("Created file '%s'", f.getAbsolutePath());
            this.csvWriters[containerId].append(String.join(",", HEADER));
            this.csvWriters[containerId].append("\n");
        }
    }

    @Override
    public void execute() throws Exception {
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        output("Container Count = %d", this.containerCount);

        // Get the storage using the config.
        storage.initialize(CONTAINER_EPOCH);
        output("Loaded %s Storage.", getServiceConfig().getStorageImplementation());

        // Gets total number of segments listed.
        int segmentsCount = 0;

        // create CSV files for listing the segments and their details
        createCSVFiles();

        output("Writing segments' details to the csv files...");
        Iterator<SegmentProperties> segmentIterator = storage.listSegments().get();
        while (segmentIterator.hasNext()) {
            SegmentProperties currentSegment = segmentIterator.next();

            // skip, if the segment is an attribute segment.
            if (NameUtils.isAttributeSegment(currentSegment.getName())) {
                continue;
            }

            segmentsCount++;
            int containerId = this.segToConMapper.getContainerId(currentSegment.getName());
            output(containerId + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getLength() + "\t" +
                    currentSegment.getName());
            this.csvWriters[containerId].append(currentSegment.isSealed() + "," + currentSegment.getLength() + "," +
                    currentSegment.getName() + "\n");
        }

        output("Closing all csv files...");
        for (FileWriter fileWriter : this.csvWriters) {
            fileWriter.flush();
            fileWriter.close();
        }

        output("All non-shadow segments' details have been written to the csv files.");
        output("Path to the csv files: '%s'", this.filePath);
        output("Total number of segments found = %d", segmentsCount);
        output("Done listing the segments!");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-segments", "Lists segments from storage with their name, length and sealed status.");
    }
}
