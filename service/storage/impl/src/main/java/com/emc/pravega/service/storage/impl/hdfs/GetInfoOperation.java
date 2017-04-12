/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.LoggerHelpers;
import com.emc.pravega.shared.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import java.io.IOException;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that retrieves information about a Segment.
 */
@Slf4j
class GetInfoOperation extends FileSystemOperation<String> implements Callable<SegmentProperties> {
    /**
     * Creates a new instance of the GetInfoOperation class.
     *
     * @param segmentName The name of the Segment to get information for.
     * @param context Context for the operation.
     */
    GetInfoOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public SegmentProperties call() throws IOException {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", segmentName);
        val allFiles = findAll(segmentName, true);
        val last = allFiles.get(allFiles.size() - 1);
        long length = last.getOffset() + last.getLength();
        SegmentProperties result = new StreamSegmentInformation(segmentName, length, isSealed(last), false, new ImmutableDate());
        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, segmentName, result);
        return result;
    }
}
