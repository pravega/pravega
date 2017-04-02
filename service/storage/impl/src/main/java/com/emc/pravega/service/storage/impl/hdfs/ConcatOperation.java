/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that concatenates a Segment to another.
 */
@Slf4j
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
        HDFSSegmentHandle target = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, this.offset, this.source);

        // Check for target offset.
        FileDescriptor lastFile = target.getLastFile();
        if (lastFile.getLastOffset() != this.offset) {
            throw new BadOffsetException(target.getSegmentName(), this.offset, lastFile.getLastOffset());
        }

        // Get all files for source handle (ignore handle contents and refresh from file system). Verify it is sealed.
        val sourceFiles = findAll(this.source.getSegmentName(), true);
        if (isSealed(sourceFiles.get(sourceFiles.size() - 1))) {
            throw new StreamSegmentSealedException(target.getSegmentName());
        }

        // Prepare source for concat. Add the "concat.next" attribute to each file, containing the next file in the sequence
        for (int i = 0; i < sourceFiles.size(); i++) {
            FileDescriptor current = sourceFiles.get(i);
            FileDescriptor next = i < sourceFiles.size() - 1 ? sourceFiles.get(i + 1) : current;
            setConcatNext(current, next);
        }

        // Prepare target for concat. Make the last file read-only, validate not fenced out and add the "concat.next"
        // attribute to the last file.
        makeReadOnly(lastFile);
        checkForFenceOut(target.getSegmentName(), target.getFiles().size(), lastFile);
        setConcatNext(lastFile, sourceFiles.get(0));

        // Invoke the common concat algorithm (shared with OpenReadOperation, OpenWriteOperation).
        resumeConcatenation(this.target);

        LoggerHelpers.traceLeave(log, "concat", traceId, target, this.offset, this.source);
    }
}
