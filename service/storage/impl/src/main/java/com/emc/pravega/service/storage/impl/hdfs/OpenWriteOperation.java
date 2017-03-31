/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that attempts to acquire an exclusive lock for a Segment.
 */
@Slf4j
class OpenWriteOperation extends FileSystemOperation<String> implements Callable<HDFSSegmentHandle> {
    private static final int MAX_OPEN_WRITE_RETRIES = 10;

    /**
     * Creates a new instance of the OpenWriteOperation class.
     *
     * @param segmentName The name of the Segment to open a WriteHandle for.
     * @param context     Context for the operation.
     */
    OpenWriteOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public HDFSSegmentHandle call() throws IOException, StorageNotPrimaryException {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", segmentName);

        HDFSSegmentHandle result = null;
        int retryCount = 0;
        while (result == null && retryCount < MAX_OPEN_WRITE_RETRIES) {
            // We care mostly about the last file in the sequence; we use this one to implement fencing.
            val allFiles = findAll(segmentName, true);
            val lastFile = allFiles.get(allFiles.size() - 1);
            if (lastFile.getEpoch() > this.context.epoch) {
                // Something unusual happened. A newer instance of the owning container had/has ownership of this segment,
                // so we cannot possibly reacquire it. This is regardless of whether the last file is read-only or not.
                throw new StorageNotPrimaryException(segmentName,
                        String.format("Found a file with a higher epoch (%d) than ours (%d): %s.",
                                lastFile.getEpoch(), this.context.epoch, lastFile.getPath()));
            }

            val offset = lastFile.getOffset() + lastFile.getLength();
            if (lastFile.isReadOnly()) {
                boolean sealed = isSealed(lastFile);
                if (sealed) {
                    // The last file is read-only and has the 'sealed' flag. This segment is sealed, as such, we cannot
                    // open it for writing, therefore open a read-only handle.
                    result = HDFSSegmentHandle.read(segmentName, allFiles);
                } else if (lastFile.getEpoch() == this.context.epoch) {
                    // The only way we can get in this state is if someone else fenced us out.
                    throw new StorageNotPrimaryException(segmentName,
                            String.format("Last file has our epoch (%d) but it is read-only: %s.", this.context.epoch, lastFile.getPath()));
                } else {
                    // The last file is read-only and not sealed. This segment is fenced off and we can continue using it.
                    return fenceOut(segmentName, offset);
                }
            } else {
                if (lastFile.getEpoch() == this.context.epoch) {
                    // The last file is not read-only and has the same epoch as us: We were the last owners of this segment;
                    // simply reuse the last file.
                    result = HDFSSegmentHandle.write(segmentName, allFiles);
                } else {
                    // The last file has a lower epoch than us. Mark it as read-only, which should fence it off.
                    makeReadOnly(lastFile);

                    // Since the state of the last segment may have changed (new writes), we need to re-do the entire
                    // algorithm to pick up any new changes. This will also reduce the chances of collision with other
                    // competing instances of this container - eventually one of them will win based on the epoch.
                    // By not setting a result, we will force the containing loop to run one more time.
                }
            }

            retryCount++;
        }

        if (result == null) {
            throw new StorageNotPrimaryException(segmentName, "Unable to acquire exclusive lock after the maximum number of attempts have been reached.");
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId, result);
        return result;
    }

    private HDFSSegmentHandle fenceOut(String segmentName, long offset) throws IOException, StorageNotPrimaryException {
        // Create a new, empty file, and verify nobody else beat us to it.
        val newFile = new FileDescriptor(getFileName(segmentName, offset, this.context.epoch), offset, 0, this.context.epoch, false);
        createEmptyFile(newFile.getPath());
        List<FileDescriptor> allFiles;
        try {
            allFiles = checkForFenceOut(segmentName, -1, newFile);
            FileDescriptor lastFile = allFiles.size() == 0 ? null : allFiles.get(allFiles.size() - 1);
            if (lastFile != null && lastFile.getEpoch() > this.context.epoch) {
                throw new StorageNotPrimaryException(segmentName,
                        String.format("Found a file with a higher epoch (%d) than ours (%d): %s.",
                                lastFile.getEpoch(), this.context.epoch, lastFile.getPath()));
            }
        } catch (StorageNotPrimaryException ex) {
            // We lost :(
            deleteFile(newFile);
            throw ex;
        }

        // It is possible that two competing containers get this far for the same segment and both believe they are owners of the lock.
        // To alleviate this, make sure all existing files with a lower epoch are marked as read-only - this should prevent
        // the lower-epoch owner to do any damage.
        val resultFiles = new ArrayList<FileDescriptor>();
        for (FileDescriptor existingFile : allFiles) {
            try {
                if (existingFile.getEpoch() < this.context.epoch) {
                    if (existingFile.getLength() == 0) {
                        deleteFile(existingFile);
                        continue;
                    } else {
                        makeReadOnly(existingFile);
                    }
                }
                resultFiles.add(existingFile);
            } catch (FileNotFoundException ex) {
                log.warn("File {} was removed, unable include it in the post-fence check.", existingFile, ex);
            }
        }

        return HDFSSegmentHandle.write(segmentName, resultFiles);
    }
}
