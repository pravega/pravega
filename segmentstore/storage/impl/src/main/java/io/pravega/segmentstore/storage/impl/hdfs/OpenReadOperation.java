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
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
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
