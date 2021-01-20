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

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.Cleanup;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;

public class SegmentReader extends DataRecoveryCommand{

    private static final int CONTAINER_EPOCH = 1;
    private final ScheduledExecutorService scheduledExecutorService = getCommandArgs().getState().getExecutor();
    private final StorageFactory storageFactory;
    private FileOutputStream fileOutputStream;
    private String filePath;
    private String segmentName;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

    /**
     * Creates an instance of StorageListSegmentsCommand class.
     *
     * @param args The arguments for the command.
     */
    public SegmentReader(CommandArgs args) {
        super(args);
        this.storageFactory = createStorageFactory(this.scheduledExecutorService);
    }

    /**
     * Creates a csv file for each container. All segments belonging to a containerId have their details written to the
     * csv file for that container.
     *
     * @throws Exception   When failed to create/delete file(s).
     */
    private void createFileAndDirectory() throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        // Set up directory for storing csv files
        this.filePath = System.getProperty("user.dir") + Path.SEPARATOR + descriptor().getName() + "_" + fileSuffix;

        // If path given as command args, use it
        ensureArgCount(1);
        this.segmentName = getCommandArgs().getArgs().get(0);
        if (getArgCount() >= 2) {
            this.filePath = getCommandArgs().getArgs().get(1);
            if (this.filePath.endsWith(Path.SEPARATOR)) {
                this.filePath = this.filePath.substring(0, this.filePath.length()-1);
            }
        }

        // Create a directory for storing files.
        File dir = new File(this.filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        File f = new File(this.filePath + Path.SEPARATOR + "segmentContent" + ".txt");
        if (f.exists()) {
            outputInfo("File '%s' already exists.", f.getAbsolutePath());
            if (!f.delete()) {
                outputError("Failed to delete the file '%s'.", f.getAbsolutePath());
                throw new Exception("Failed to delete the file " + f.getAbsolutePath());
            }
        }
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
        ContainerRecoveryUtils.readSegment(storage, this.segmentName, this.fileOutputStream, scheduledExecutorService, TIMEOUT);

        outputInfo("Closing the file...");
        this.fileOutputStream.close();

        outputInfo("The segment's contents have been written to the file.");
        outputInfo("Path to the csv files: '%s'", this.filePath);
        outputInfo("Done!");
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "write-segment", "Writes the contents of the segment to a file.");
    }

}
