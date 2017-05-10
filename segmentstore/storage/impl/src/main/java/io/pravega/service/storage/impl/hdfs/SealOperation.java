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
import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * FileSystemOperation that Seals a Segment.
 */
@Slf4j
class SealOperation extends FileSystemOperation<HDFSSegmentHandle> implements RunnableWithException {
    /**
     * Creates a new instance of the SealOperation class.
     *
     * @param handle  A WriteHandle containing information about the Segment to seal.
     * @param context Context for the operation.
     */
    SealOperation(HDFSSegmentHandle handle, OperationContext context) {
        super(handle, context);
    }

    @Override
    public void run() throws IOException, StorageNotPrimaryException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        val lastHandleFile = handle.getLastFile();
        try {
            if (!lastHandleFile.isReadOnly()) {
                if (!makeReadOnly(lastHandleFile)) {
                    // The file's read-only status changed externally. Figure out if we have been fenced out.
                    checkForFenceOut(handle.getSegmentName(), -1, handle.getLastFile());

                    // We are ok, just update the FileDescriptor internally.
                    lastHandleFile.markReadOnly();
                }
            }

            // Set the Sealed attribute on the last file and update the handle.
            makeSealed(lastHandleFile);
        } catch (FileNotFoundException | AclException ex) {
            checkForFenceOut(handle.getSegmentName(), handle.getFiles().size(), handle.getLastFile());
            throw ex; // If we were not fenced out, then this is a legitimate exception - rethrow it.
        }

        if (lastHandleFile.getLength() == 0) {
            // Last file was actually empty, so if we have more than one file, mark the second-to-last as sealed and
            // remove the last one.
            val handleFiles = handle.getFiles();
            if (handleFiles.size() > 1) {
                makeSealed(handleFiles.get(handleFiles.size() - 2));
                deleteFile(lastHandleFile);
                handle.removeLastFile();
            }
        }

        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }
}
