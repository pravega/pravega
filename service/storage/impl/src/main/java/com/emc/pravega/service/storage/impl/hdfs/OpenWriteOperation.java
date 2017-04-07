/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import java.io.IOException;
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
        int attemptCount = 0;
        while (result == null && attemptCount < MAX_OPEN_WRITE_RETRIES) {
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

            if (lastFile.isReadOnly()) {
                if (isSealed(lastFile)) {
                    // The last file is read-only and has the 'sealed' flag. This segment is sealed, as such, we cannot
                    // open it for writing, therefore open a read-only handle.
                    result = HDFSSegmentHandle.read(segmentName, allFiles);
                } else if (lastFile.getEpoch() == this.context.epoch) {
                    // This means someone else must have just fenced us out.
                    throw new StorageNotPrimaryException(segmentName,
                            String.format("Last file has our epoch (%d) but it is read-only: %s.", this.context.epoch, lastFile.getPath()));
                } else {
                    // The last file is read-only and not sealed. This segment is fenced off and we can continue using it.
                    result = fenceOut(segmentName, lastFile.getLastOffset());
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

            attemptCount++;
        }

        if (result == null) {
            throw new StorageNotPrimaryException(segmentName, "Unable to acquire exclusive lock after the maximum number of attempts have been reached.");
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId, result);
        return result;
    }

    private HDFSSegmentHandle fenceOut(String segmentName, long offset) throws IOException, StorageNotPrimaryException {
        // Create a new, empty file, and verify nobody else beat us to it.
        val newFile = new FileDescriptor(getFilePath(segmentName, offset, this.context.epoch), offset, 0, this.context.epoch, false);
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

        // At this point it is possible that two competing containers get this far for the same segment and both believe
        // they are owners of the lock.
        //
        // To handle this, we consolidate all previous files into a single one and ensure that file is read-only. If that
        // file ends up being empty, we delete it. As such:
        // 1. Competing containers with lower epochs will not be able to do any damage.
        // 2. The number of files for a segment is always either 1 or 2, regardless of how many calls to OpenWrite we make
        cleanup(allFiles);
        return HDFSSegmentHandle.write(segmentName, allFiles);
    }

    /**
     * Compacts all but the last file into the first file (by means of concatenation). If the resulting file would be
     * empty, it will be deleted.
     *
     * @param allFiles An ordered Sequence of FileDescriptors to operate on. This List will be modified to reflect the
     *                 new state of the FileSystem after this method completes.
     * @throws IOException If an Exception occurred.
     */
    private void cleanup(List<FileDescriptor> allFiles) throws IOException {
        if (allFiles.size() > 2) {
            // We keep the last file alone - that is the file we just created above, and there is no point of combining
            // a file to itself.
            val lastFile = allFiles.get(allFiles.size() - 1);
            FileDescriptor combinedFile = combine(allFiles.get(0), allFiles.subList(1, allFiles.size() - 1), true);

            assert combinedFile.getLastOffset() == lastFile.getOffset()
                    : String.format("New file list would not be contiguous. Combined file ends at offset %d; Last file begins at %d.",
                    combinedFile.getLastOffset(), lastFile.getOffset());

            allFiles.clear();
            allFiles.add(combinedFile);
            allFiles.add(lastFile);
        }

        if (allFiles.size() > 1) {
            val firstFile = allFiles.get(0);
            if (firstFile.getLength() == 0) {
                // First file is empty. No point in keeping it around.
                deleteFile(firstFile);
                allFiles.remove(0);
            } else if (!firstFile.isReadOnly()) {
                // First file is not read-only. It should be.
                makeReadOnly(allFiles.get(0));
            }
        }
    }
}
