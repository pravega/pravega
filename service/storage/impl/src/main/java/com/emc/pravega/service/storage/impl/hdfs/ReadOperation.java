/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * FileSystemOperation that Reads from a Segment.
 */
public class ReadOperation extends FileSystemOperation<HDFSSegmentHandle> implements Callable<Integer> {
    private final long offset;
    private final byte[] buffer;
    private final int bufferOffset;
    private final int length;

    /**
     * Creates a new instance of the ReadOperation class.
     *
     * @param handle       A Read or ReadWrite handle for the Segment to read from.
     * @param offset       The offset in the Segment to begin reading at.
     * @param buffer       A buffer to load read data into.
     * @param bufferOffset An offset in the buffer to start loading the data at.
     * @param length       The number of bytes to read.
     * @param context Context for the operation.
     */
    ReadOperation(HDFSSegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, OperationContext context) {
        super(handle, context);
        this.offset = offset;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.length = length;
    }

    @Override
    public Integer call() throws IOException {
        return null;
    }
}
