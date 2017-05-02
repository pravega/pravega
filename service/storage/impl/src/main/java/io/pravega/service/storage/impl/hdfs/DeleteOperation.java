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

import io.pravega.common.LoggerHelpers;
import io.pravega.common.function.RunnableWithException;
import io.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that Deletes a Segment.
 */
@Slf4j
class DeleteOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    /**
     * Creates a new instance of the DeleteOperation class.
     *
     * @param handle  A WriteHandle containing information about the segment to delete.
     * @param context Context for the operation.
     */
    DeleteOperation(HDFSSegmentHandle handle, OperationContext context) {
        super(handle, context);
    }

    @Override
    public void run() throws IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
        ensureCanDelete(handle);

        // Get an initial list of all files.
        List<FileDescriptor> files = handle.getFiles();
        while (files.size() > 0) {
            // Just in case the last file is not read-only, mark it as such, to prevent others from writing to it.
            val lastFile = files.get(files.size() - 1);
            try {
                if (!makeReadOnly(lastFile)) {
                    // Last file was already readonly.
                    checkForFenceOut(handle.getSegmentName(), -1, lastFile);
                }
            } catch (FileNotFoundException ex) {
                checkForFenceOut(handle.getSegmentName(), -1, lastFile);
                throw ex;
            }

            // Delete every file in this set.
            for (FileDescriptor f : files) {
                log.debug("Deleting file {}.", f);
                try {
                    deleteFile(f);
                } catch (IOException ex) {
                    log.warn("Could not delete {}.", f, ex);
                }
            }

            // In case someone else created a new (set of) files for this segment while we were working, remove them all too.
            files = findAll(handle.getSegmentName(), false);
        }

        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }

    /**
     * Ensures the given handle can be used for deleting a segment. Conditions:
     * 1. Handle must be read-write OR
     * 2. Segment must be sealed.
     */
    private void ensureCanDelete(HDFSSegmentHandle handle) throws IOException {
        boolean canDelete = !handle.isReadOnly();
        if (!canDelete) {
            canDelete = isSealed(handle.getLastFile());
        }

        Preconditions.checkArgument(canDelete, "Cannot delete using a read-only handle, unless the segment is sealed.");
    }
}
