/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
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
        if (lastFile.isReadOnly()) {
            // Don't bother going to the server; we know we can't write to this one.
            throw HDFSExceptionHelpers.segmentSealedException(handle.getSegmentName());
        }

        if (this.offset != lastFile.getLastOffset()) {
            throw new BadOffsetException(handle.getSegmentName(), lastFile.getLastOffset(), this.offset);
        }

        try (FSDataOutputStream stream = this.context.fileSystem.append(new Path(lastFile.getPath()))) {
            if (stream.getPos() != lastFile.getLength()) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                // Update our knowledge of the filesystem and throw a BadOffsetException - this should cause upstream code
                // to try to reconcile; if it can't then the upstream code should shut down or take other appropriate measures.
                log.warn("File changed detected for '{}'. Expected length = {}, actual length = {}.", lastFile, lastFile.getLength(), stream.getPos());
                lastFile.setLength(stream.getPos());
                throw new BadOffsetException(handle.getSegmentName(), lastFile.getLastOffset(), this.offset);
            }

            IOUtils.copyBytes(this.data, stream, this.length);
            stream.flush();
            lastFile.increaseLength(this.length);
        } catch (FileNotFoundException | AclException ex) {
            checkForFenceOut(handle);
            throw ex; // If we were not fenced out, then this is a legitimate exception - rethrow it.
        }

        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, length);
    }
}
