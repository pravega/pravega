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
        ensureArgCount(1);
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
        int countEvents = readSegment(storage, this.segmentName, TIMEOUT);
        outputInfo("Number of events found = %d", countEvents);

        outputInfo("Closing the file...");
        this.fileOutputStream.close();

        outputInfo("The segment's contents have been written to the file.");
        outputInfo("Path to the file '%s'", this.filePath);
        outputInfo("Done!");
    }

    public int readSegment(Storage storage, String segmentName, Duration timeout)
            throws Exception {

        val segmentInfo = storage.getStreamSegmentInfo(segmentName, timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        int bytesToRead = (int) segmentInfo.getLength();
        val sourceHandle = storage.openRead(segmentName).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        byte[] buffer = new byte[bytesToRead];
        storage.read(sourceHandle, 0, buffer, 0, bytesToRead, timeout)
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        int countEvents = 0;
        int offset = 0;
        while (offset < bytesToRead) {
            outputInfo("Offset = %d", offset);
            byte[] header = Arrays.copyOfRange(buffer, offset, offset + 8);
            long length = convertByteArrayToLong(header);
            offset += length + 8;
            countEvents++;
        }
        return countEvents;
    }

    private long convertByteArrayToLong(byte[] longBytes){
        ByteBuffer byteBuffer = ByteBuffer.wrap(longBytes);
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "write-segment", "Writes the contents of the segment to a file.");
    }

}
