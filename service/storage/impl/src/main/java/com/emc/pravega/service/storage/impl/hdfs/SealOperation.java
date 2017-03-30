/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that Seals a Segment.
 */
@Slf4j
class SealOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    /**
     * Creates a new instance of the SealOperation class.
     *
     * @param handle  A WriteHandle containing information about the Segment to seal.
     * @param context Context for the operation.
     */
    SealOperation(HDFSSegmentHandle handle, OperationContext context) {
        super(handle, context);
    }

    @Override
    public void run() throws IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        val lastHandleFile = handle.getLastFile();
        if (!lastHandleFile.isReadOnly()) {
            if (!makeReadOnly(lastHandleFile)) {
                // The file's read-only status changed externally. Figure out if we have been fenced out.
                checkForFenceOut(handle);

                // We are ok, just update the FileInfo internally.
                lastHandleFile.markReadOnly();
            }
        }

        // Set the Sealed attribute on the last file and update the handle.
        makeSealed(lastHandleFile);
        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }
}
