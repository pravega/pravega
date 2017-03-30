/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * FileSystemOperation that appends to a Segment.
 */
@Slf4j
public class WriteOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    private final long offset;
    private final InputStream data;
    private final int length;

    /**
     * Creates a new instance of the WriteOperation class.
     *
     * @param handle  A WriteHandle that contains information about the Segment to write to.
     * @param offset  The offset within the segment to write at.
     * @param data    An InputStream representing the data to write.
     * @param length  The number of bytes to write.
     * @param context Context for the operation.
     */
    WriteOperation(HDFSSegmentHandle handle, long offset, InputStream data, int length, OperationContext context) {
        super(handle, context);
        this.offset = offset;
        this.data = data;
        this.length = length;
    }

    @Override
    public void run() throws BadOffsetException, IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "write", handle, this.offset, this.length);
        FileInfo lastFile = handle.getLastFile();
        if (lastFile.isReadOnly()) {
            if (isSealed(lastFile)) {
                // 1. Segment is sealed. Nothing more we can do.
                throw new AclException(handle.getSegmentName());
            }

            // 2. This file was fenced out using openWrite by us or some other instance, so figure it out.
            if (lastFile.getEpoch() <= this.context.epoch) {
                // Determine if the filesystem has changed since the last time we checked it (if someone else
                val systemFiles = findAll(handle.getSegmentName(), true);
                val lastSystemFile = systemFiles.get(systemFiles.size() - 1);
                if (lastSystemFile.getEpoch() > lastFile.getEpoch()) {
                    // The last file's epoch in the file system is higher than ours. We have been fenced out.
                    throw new StorageNotPrimaryException(handle.getSegmentName(),
                            String.format("Last file in FileSystem (%s) has a higher epoch than that of ours (%s).",
                                    lastSystemFile, lastFile));
                } else {
                    makeReadOnly(lastSystemFile);
                    handle.update(systemFiles);
                }
            } else {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
        }

        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, length);
    }
}
