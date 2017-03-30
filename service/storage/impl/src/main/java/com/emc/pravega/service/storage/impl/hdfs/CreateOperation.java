/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * FileSystemOperation that creates a new Segment.
 */
@Slf4j
class CreateOperation extends FileSystemOperation<String> implements Callable<SegmentProperties> {
    /**
     * Creates a new instance of the CreateOperation class.
     *
     * @param segmentName The name of the Segment to create.
     * @param context     Context for the operation.
     */
    CreateOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public SegmentProperties call() throws IOException, StreamSegmentException {
        // Create the segment using our own epoch.
        String segmentName = getTarget();
        String fullPath = getFileName(segmentName, 0, this.context.epoch);
        long traceId = LoggerHelpers.traceEnter(log, "create", segmentName, fullPath);
        this.context.fileSystem
                .create(new Path(fullPath),
                        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                        false,
                        0,
                        this.context.config.getReplication(),
                        this.context.config.getBlockSize(),
                        null)
                .close();

        // Determine if someone also created it at the same time, but with a different epoch.
        try {
            checkForFenceOut(segmentName, 1, new FileInfo(fullPath, 0, 0, this.context.epoch, false));
        } catch (StorageNotPrimaryException ex) {
            // We lost :(
            this.context.fileSystem.delete(new Path(fullPath), true);
            throw ex;
        }

        LoggerHelpers.traceLeave(log, "create", traceId, segmentName);
        return new StreamSegmentInformation(segmentName, 0, false, false, new ImmutableDate());
    }
}
