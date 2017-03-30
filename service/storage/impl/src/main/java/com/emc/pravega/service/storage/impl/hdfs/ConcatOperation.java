/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;

/**
 * FileSystemOperation that concatenates a Segment to another.
 */
public class ConcatOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    private final long offset;
    private final HDFSSegmentHandle source;

    /**
     * Creates a new instance of the ConcatOperation class.
     *
     * @param target  A WriteHandle containing information about the Segment to concat TO.
     * @param offset  The offset in the target to concat at.
     * @param source  A Handle containing information about the Segment to concat FROM.
     * @param context Context for the operation.
     */
    ConcatOperation(HDFSSegmentHandle target, long offset, HDFSSegmentHandle source, OperationContext context) {
        super(target, context);
        this.offset = offset;
        this.source = source;
    }

    @Override
    public void run() throws IOException, BadOffsetException, StreamSegmentSealedException, StorageNotPrimaryException {

    }
}
