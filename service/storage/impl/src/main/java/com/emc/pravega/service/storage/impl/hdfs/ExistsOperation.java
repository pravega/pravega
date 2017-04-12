/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.LoggerHelpers;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that determines whether a Segment exists.
 */
@Slf4j
public class ExistsOperation extends FileSystemOperation<String> implements Callable<Boolean> {
    /**
     * Creates a new instance of the ExistsOperation class.
     *
     * @param segmentName The name of the Segment to check existence for.
     * @param context     Context for the operation.
     */
    ExistsOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public Boolean call() throws Exception {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "exists", segmentName);
        val files = findAll(segmentName, false);
        boolean exists = files.size() > 0;
        LoggerHelpers.traceLeave(log, "exists", traceId, segmentName, exists);
        return exists;
    }
}
