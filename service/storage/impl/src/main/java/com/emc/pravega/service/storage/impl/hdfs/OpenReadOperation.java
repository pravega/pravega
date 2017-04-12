/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.LoggerHelpers;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that opens a Segment in ReadOnly mode.
 */
@Slf4j
class OpenReadOperation extends FileSystemOperation<String> implements Callable<HDFSSegmentHandle> {
    /**
     * Creates a new instance of the OpenReadOperation class.
     *
     * @param segmentName The name of the Segment to open a ReadHandle for.
     * @param context Context for the operation.
     */
    OpenReadOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public HDFSSegmentHandle call() throws IOException, StorageNotPrimaryException {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "openRead", segmentName);
        val allFiles = findAll(segmentName, true);
        val result = HDFSSegmentHandle.read(segmentName, allFiles);
        LoggerHelpers.traceLeave(log, "openRead", traceId, result);
        return result;
    }
}
