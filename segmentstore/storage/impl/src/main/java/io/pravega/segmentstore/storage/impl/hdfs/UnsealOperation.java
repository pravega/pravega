/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.common.LoggerHelpers;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * FileSystemOperation that Unseals a Segment.
 */
@Slf4j
public class UnsealOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    /**
     * Creates a new instance of the UnsealOperation class.
     *
     * @param handle  A WriteHandle containing information about the Segment to unseal.
     * @param context Context for the operation.
     */
    UnsealOperation(HDFSSegmentHandle handle, OperationContext context) {
        super(handle, context);
    }

    @Override
    public void run() throws IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "unseal", handle);
        val lastHandleFile = handle.getLastFile();
        try {
            if (lastHandleFile.isReadOnly()) {
                makeReadWrite(lastHandleFile);
            }

            // Set the Sealed attribute on the last file and update the handle.
            if (isSealed(lastHandleFile)) {
                makeUnsealed(lastHandleFile);
            }
        } catch (FileNotFoundException | AclException ex) {
            checkForFenceOut(handle.getSegmentName(), handle.getFiles().size(), handle.getLastFile());
            throw ex; // If we were not fenced out, then this is a legitimate exception - rethrow it.
        }

        LoggerHelpers.traceLeave(log, "unseal", traceId, handle);
    }
}
