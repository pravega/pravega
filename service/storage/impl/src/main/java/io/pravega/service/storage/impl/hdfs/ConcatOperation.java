/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.service.storage.impl.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.function.RunnableWithException;
import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * FileSystemOperation that concatenates a Segment to another.
 */
@Slf4j
public class ConcatOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    private final long offset;
    private final String sourceSegmentName;

    /**
     * Creates a new instance of the ConcatOperation class that will attempt to start a concat operation from the beginning.
     *
     * @param target            A WriteHandle containing information about the Segment to concat TO.
     * @param offset            The offset in the target to concat at.
     * @param sourceSegmentName The name of the Segment to concatenate.
     * @param context           Context for the operation.
     */
    ConcatOperation(HDFSSegmentHandle target, long offset, String sourceSegmentName, OperationContext context) {
        super(target, context);
        Preconditions.checkArgument(!target.getSegmentName().equals(sourceSegmentName), "Source and Target are the same segment.");
        this.offset = offset;
        this.sourceSegmentName = sourceSegmentName;
    }

    @Override
    public void run() throws IOException, BadOffsetException, StreamSegmentSealedException, StorageNotPrimaryException {
        HDFSSegmentHandle target = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, this.offset, this.sourceSegmentName);

        // Check for target offset and whether it is sealed.
        FileDescriptor lastFile = this.target.getLastFile();
        validate(lastFile);

        // Get all files for source handle (ignore handle contents and refresh from file system). Verify it is sealed.
        val sourceFiles = findAll(this.sourceSegmentName, true);
        Preconditions.checkState(isSealed(sourceFiles.get(sourceFiles.size() - 1)),
                "Cannot concat segment '%s' into '%s' because it is not sealed.", this.sourceSegmentName);

        if (sourceFiles.get(sourceFiles.size() - 1).getLastOffset() == 0) {
            // Quick bail-out: source segment is empty, simply delete it.
            log.debug("Source Segment '%s' is empty. No concat will be performed. Source Segment will be deleted.", this.sourceSegmentName);
            val readHandle = HDFSSegmentHandle.read(this.sourceSegmentName, sourceFiles);
            new DeleteOperation(readHandle, this.context).run();
            LoggerHelpers.traceLeave(log, "concat", traceId, this.target, this.offset, this.sourceSegmentName);
            return;
        }

        try {
            // Concat source files into target and update the handle.
            FileDescriptor newLastFile = combine(lastFile, sourceFiles, false);
            this.target.replaceLastFile(newLastFile);
        } catch (FileNotFoundException | AclException ex) {
            checkForFenceOut(this.target.getSegmentName(), this.target.getFiles().size(), lastFile);
            throw ex; // If we were not fenced out, then this is a legitimate exception - rethrow it.
        }

        LoggerHelpers.traceLeave(log, "concat", traceId, target, this.offset, this.sourceSegmentName);
    }

    /**
     * Ensures that the ConcatOperation is valid based on the state of the given last file.
     */
    private void validate(FileDescriptor lastFile) throws IOException, BadOffsetException, StorageNotPrimaryException {
        try {
            if (isSealed(lastFile)) {
                throw HDFSExceptionHelpers.segmentSealedException(this.target.getSegmentName());
            } else if (lastFile.getLastOffset() != this.offset) {
                throw new BadOffsetException(this.target.getSegmentName(), lastFile.getLastOffset(), this.offset);
            }
        } catch (FileNotFoundException fnf) {
            // The last file could have been deleted due to being fenced out (i.e., empty file).
            checkForFenceOut(this.target.getSegmentName(), this.target.getFiles().size(), lastFile);
            throw fnf;
        }
    }
}
