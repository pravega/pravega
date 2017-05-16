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
import io.pravega.common.Timer;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;

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
        FileDescriptor lastFile = handle.getLastFile();

        Timer timer = new Timer();
        try (FSDataOutputStream stream = this.context.fileSystem.append(lastFile.getPath())) {
            if (this.offset != lastFile.getLastOffset()) {
                // Do the handle offset validation here, after we open the file. We want to throw FileNotFoundException
                // before we throw BadOffsetException.
                throw new BadOffsetException(handle.getSegmentName(), lastFile.getLastOffset(), this.offset);
            } else if (stream.getPos() != lastFile.getLength()) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                // Update our knowledge of the filesystem and throw a BadOffsetException - this should cause upstream code
                // to try to reconcile; if it can't then the upstream code should shut down or take other appropriate measures.
                log.warn("File changed detected for '{}'. Expected length = {}, actual length = {}.", lastFile, lastFile.getLength(), stream.getPos());
                lastFile.setLength(stream.getPos());
                throw new BadOffsetException(handle.getSegmentName(), lastFile.getLastOffset(), this.offset);
            }

            if (this.length == 0) {
                // Exit here (vs at the beginning of the method), since we want to throw appropriate exceptions in case
                // of Sealed or BadOffset
                // Note: IOUtils.copyBytes with length == 0 will enter an infinite loop, hence the need for this check.
                return;
            }

            IOUtils.copyBytes(this.data, stream, this.length);
            stream.flush();
            lastFile.increaseLength(this.length);
        } catch (FileNotFoundException | AclException ex) {
            checkForFenceOut(handle.getSegmentName(), handle.getFiles().size(), handle.getLastFile());
            throw ex; // If we were not fenced out, then this is a legitimate exception - rethrow it.
        }

        Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        Metrics.WRITE_BYTES.add(this.length);
        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, length);
    }
}
