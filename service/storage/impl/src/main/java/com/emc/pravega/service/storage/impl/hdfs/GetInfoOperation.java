/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that retrieves information about a Segment.
 */
@Slf4j
class GetInfoOperation extends FileSystemOperation<String> implements Callable<SegmentProperties> {
    private static final int MAX_ATTEMPT_COUNT = 3;

    /**
     * Creates a new instance of the GetInfoOperation class.
     *
     * @param segmentName The name of the Segment to get information for.
     * @param context     Context for the operation.
     */
    GetInfoOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public SegmentProperties call() throws IOException {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", segmentName);
        List<FileDescriptor> allFiles = findAll(segmentName, true);
        SegmentProperties result = null;
        int attemptCount = 0;
        do {
            val last = allFiles.get(allFiles.size() - 1);
            long length = last.getOffset() + last.getLength();
            boolean isSealed;
            try {
                isSealed = isSealed(last);
            } catch (FileNotFoundException fnf) {
                // This can happen if we get a concurrent call to SealOperation with an empty last file; the last file will
                // be deleted in that case so we need to try our luck again (in which case we need to refresh the file list).
                if (++attemptCount < MAX_ATTEMPT_COUNT) {
                    allFiles = findAll(segmentName, true);
                    continue;
                }

                throw fnf;
            }

            result = new StreamSegmentInformation(segmentName, length, isSealed, false, new ImmutableDate());
        } while (result == null);
        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, segmentName, result);
        return result;
    }
}
