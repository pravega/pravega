/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.Path;

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
    public SegmentProperties call() throws IOException, StorageNotPrimaryException {
        // Create the segment using our own epoch.
        String segmentName = getTarget();
        val existingFiles = findAllRaw(segmentName);
        if (existingFiles != null && existingFiles.length > 0) {
            // Segment already exists; don't bother with anything else.
            throw HDFSExceptionHelpers.segmentExistsException(segmentName);
        }

        // Create the first file in the segment.
        String fullPath = getFileName(segmentName, 0, this.context.epoch);
        long traceId = LoggerHelpers.traceEnter(log, "create", segmentName, fullPath);
        createEmptyFile(fullPath);

        // Determine if someone also created it at the same time, but with a different epoch.
        List<FileDescriptor> allFiles;
        try {
            allFiles = checkForFenceOut(segmentName, 1, new FileDescriptor(fullPath, 0, 0, this.context.epoch, false));
        } catch (StorageNotPrimaryException ex) {
            // We lost :(
            this.context.fileSystem.delete(new Path(fullPath), true);
            throw ex;
        }

        // Post-creation cleanup.
        // If we find any lower-epoch files that are empty, we can remove them.
        // If we find any non-empty lower-epoch files, it means the segment already exists (someone else created it and
        // wrote data to it while we were still checking).
        for (FileDescriptor existingFile : allFiles) {
            try {
                if (existingFile.getEpoch() < this.context.epoch) {
                    if (existingFile.getLength() == 0) {
                        deleteFile(existingFile);
                    } else {
                        // Back off the change in case we found a non-empty file with lower epoch.
                        this.context.fileSystem.delete(new Path(fullPath), true);
                        throw HDFSExceptionHelpers.segmentExistsException(segmentName);
                    }
                }
            } catch (FileNotFoundException ex) {
                log.warn("File {} was removed, unable include it in the post-fence check.", existingFile, ex);
            }
        }

        LoggerHelpers.traceLeave(log, "create", traceId, segmentName);
        return new StreamSegmentInformation(segmentName, 0, false, false, new ImmutableDate());
    }
}
