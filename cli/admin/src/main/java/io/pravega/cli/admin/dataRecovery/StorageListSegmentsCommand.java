/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.Cleanup;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

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
    private final ScheduledExecutorService scheduledExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(10,
            "listSegmentsProcessor");
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
        this.segToConMapper = new SegmentToContainerMapper(this.containerCount);
        this.storageFactory = createStorageFactory(scheduledExecutorService);
        this.csvWriters = new FileWriter[this.containerCount];
    }

    /**
     * Creates a csv file for each container. All segments belonging to a containerId have their details written to the
     * csv file for that container.
     *
     * @throws Exception   When failed to create/delete file(s).
     */
    private void createCSVFiles() throws Exception {
        for (int containerId = 0; containerId < this.containerCount; containerId++) {
            File f = new File(this.filePath + "/" + "Container_" + containerId + ".csv");
            if (f.exists()) {
                output(Level.FINE, "File '%s' already exists.", f.getAbsolutePath());
                if (!f.delete()) {
                    output(Level.SEVERE, "Failed to delete the file '%s'.", f.getAbsolutePath());
                    throw new Exception("Failed to delete the file " + f.getAbsolutePath());
                }
            }
            if (!f.createNewFile()) {
                output(Level.SEVERE, "Failed to create file '%s'.", f.getAbsolutePath());
                throw new Exception("Failed to create file " + f.getAbsolutePath());
            }
            this.csvWriters[containerId] = new FileWriter(f.getName());
            output(Level.INFO, "Created file '%s'", f.getAbsolutePath());
            this.csvWriters[containerId].append(String.join(",", HEADER));
            this.csvWriters[containerId].append("\n");
        }
    }

    @Override
    public void execute() throws Exception {
        // set up logging
        this.filePath = setLogging(descriptor().getName());

        output(Level.INFO, "Container Count = %d", this.containerCount);
        // Get the storage using the config.
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        storage.initialize(CONTAINER_EPOCH);
        output(Level.INFO, "Loaded %s Storage.", getServiceConfig().getStorageImplementation().toString());

        // Gets total number of segments listed.
        int segmentsCount = 0;

        createCSVFiles();

        output(Level.INFO, "Writing segments' details to the csv files...");
        Iterator<SegmentProperties> segmentIterator = storage.listSegments();
        while (segmentIterator.hasNext()) {
            SegmentProperties currentSegment = segmentIterator.next();

            // skip recovery if the segment is an attribute segment.
            if (NameUtils.isAttributeSegment(currentSegment.getName())) {
                continue;
            }

            segmentsCount++;
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            output(Level.FINE, containerId + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getLength() + "\t" +
                    currentSegment.getName());
            csvWriters[containerId].append(currentSegment.isSealed() + "," + currentSegment.getLength() + "," +
                    currentSegment.getName() + "\n");
        }

        output(Level.FINE, "Closing all csv files...");
        for (int containerId = 0; containerId < containerCount; containerId++) {
            csvWriters[containerId].flush();
            csvWriters[containerId].close();
        }

        output(Level.INFO, "All non-shadow segments' details have been written to the csv files.");
        output(Level.FINE, "Path to the csv files: '%s'", filePath);
        output(Level.INFO, "Total number of segments found = %d", segmentsCount);
        output(Level.INFO, "Done listing the segments!");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-segments", "Lists segments from storage with their name, length and sealed status.");
    }
}
