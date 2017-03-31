/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * FileSystemOperation that Deletes a Segment.
 */
@Slf4j
class DeleteOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    /**
     * Creates a new instance of the DeleteOperation class.
     *
     * @param handle  A WriteHandle containing information about the segment to delete.
     * @param context Context for the operation.
     */
    DeleteOperation(HDFSSegmentHandle handle, OperationContext context) {
        super(handle, context);
    }

    @Override
    public void run() throws IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);

        // Get an initial list of all files.
        List<FileDescriptor> files = findAll(handle.getSegmentName(), true);
        while (files.size() > 0) {
            // Just in case the last file is not read-only, mark it as such, to prevent others from writing to it.
            makeReadOnly(files.get(files.size() - 1));

            // Delete every file in this set.
            for (FileDescriptor f : files) {
                log.debug("Deleting file {}.", f);
                try {
                    deleteFile(f);
                } catch (IOException ex) {
                    log.warn("Could not delete {}.", f, ex);
                }
            }

            // In case someone else created a new (set of) files for this segment while we were working, remove them all too.
            files = findAll(handle.getSegmentName(), false);
        }

        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }
}
