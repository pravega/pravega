/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.LoggerHelpers;
import com.emc.pravega.shared.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;

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

    /**
     * Creates a new instance of the ConcatOperation class that can resume a previously begun concat operation.
     *
     * @param target  A WriteHandle containing information about the Segment to concat TO.
     * @param context Context for the operation.
     */
    ConcatOperation(HDFSSegmentHandle target, OperationContext context) {
        super(target, context);
        this.offset = -1;
        this.sourceSegmentName = null;
    }

    @Override
    public void run() throws IOException, BadOffsetException, StreamSegmentSealedException, StorageNotPrimaryException {
        HDFSSegmentHandle target = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, this.offset, this.sourceSegmentName);

        // Set attributes (this helps with recovery from failures).
        boolean needsConcat = prepareConcatenation();
        if (needsConcat) {
            // Invoke the shared concat algorithm.
            resumeConcatenation();
        }

        LoggerHelpers.traceLeave(log, "concat", traceId, target, this.offset, this.sourceSegmentName);
    }

    /**
     * Sets the concat.next attributes on all files that need to be concatenated. Each file will have a concat.next
     * attribute that points to the next file that is supposed to be concatenated after it.
     * Updates the target segment to make the last file read-only.
     *
     * @return True if concat is required, false otherwise (if the source segment is empty).
     */
    @VisibleForTesting
    boolean prepareConcatenation() throws IOException, BadOffsetException, StreamSegmentSealedException, StorageNotPrimaryException {
        Preconditions.checkState(this.offset >= 0 && this.sourceSegmentName != null, "Cannot start a new Concat operation if no source segment is defined.");
        long traceId = LoggerHelpers.traceEnter(log, "prepareConcatenation", this.target);

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
            LoggerHelpers.traceLeave(log, "setConcatAttributes", traceId, this.target, false);
            return false;
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
        checkForFenceOut(this.target.getSegmentName(), this.target.getFiles().size(), lastFile);
        setConcatNext(lastFile, sourceFiles.get(0));
        LoggerHelpers.traceLeave(log, "prepareConcatenation", traceId, this.target, true);
        return true;
    }

    /**
     * Initiates or resumes a concatenation operation on the given handle.
     * This is invoked either via this operation or via the OpenWriteOperation.
     */
    void resumeConcatenation() throws IOException {
        HDFSSegmentHandle targetHandle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "resumeConcatenation", targetHandle);
        Preconditions.checkArgument(!targetHandle.isReadOnly(), "targetHandle must not be read-only.");

        // Three-stage process: 1. Gather all information, 2. Apply and 3. Cleanup.
        val renames = collect(targetHandle);
        performRenames(renames, targetHandle);
        cleanup(targetHandle);
        LoggerHelpers.traceLeave(log, "resumeConcatenation", traceId, targetHandle);
    }

    /**
     * Generates list of renames/replaces to perform, in order. This also pre-validates the input and filesystem state so we
     * don't detect a corruption mid-way and leave the segments in limbo.
     * Start with the last file of the handle, and follow the "concat.next" attribute until we no longer detect one
     * or find a file that has the 'sealed' attribute.
     */
    private List<RenamePair> collect(HDFSSegmentHandle targetHandle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "collect", targetHandle);
        val result = new ArrayList<RenamePair>();
        val seenFiles = targetHandle.getFiles().stream().map(f -> f.getPath().toString()).collect(Collectors.toSet());
        FileDescriptor current = targetHandle.getLastFile();
        long offset = current.getLastOffset();
        while (!isSealed(current)) {
            // Find sourceFile as indicated by concatNextAttribute.
            String concatNextPath = getConcatNext(current);
            if (concatNextPath == null) {
                // We are done gathering.
                break;
            }

            Path nextPath = new Path(concatNextPath);
            if (!seenFiles.add(concatNextPath)) {
                throw new SegmentFilesCorruptedException(targetHandle.getSegmentName(), current,
                        String.format("Circular dependency found. File '%s' was seen more than once.", current));
            }

            if (result.size() == 0 && current.getLength() == 0) {
                // This is the first file to process; current points to the last file in the handle, which is empty.
                // In this case, we can simply replace that file.
                result.add(new RenamePair(nextPath, current.getPath(), true));
            } else {
                // Generate the new name of the file, validate it, and record the rename mapping.
                Path newPath = getFilePath(targetHandle.getSegmentName(), offset, this.context.epoch);
                if (this.context.fileSystem.exists(newPath)) {
                    throw new FileAlreadyExistsException(newPath.toString());
                }

                result.add(new RenamePair(nextPath, newPath, false));
            }

            current = toDescriptor(this.context.fileSystem.getFileStatus(nextPath));
            offset += current.getLength();
        }

        LoggerHelpers.traceLeave(log, "collect", traceId, targetHandle, result);
        return result;
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

    /**
     * Executes the actual renames in the filesystem and updates the target handle.
     */
    private void performRenames(List<RenamePair> renames, HDFSSegmentHandle targetHandle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "performRenames", targetHandle, renames);
        for (RenamePair toRename : renames) {
            FileDescriptor lastFile = targetHandle.getLastFile();
            if (!lastFile.isReadOnly()) {
                makeReadOnly(lastFile);
            }

            if (toRename.replace) {
                this.context.fileSystem.delete(toRename.destination, true);
                if (!this.context.fileSystem.rename(toRename.source, toRename.destination)) {
                    throw new IOException(String.format("Could not rename '%s' to '%s' (after attempting to delete the latter).",
                            toRename.source, toRename.destination));
                }

                log.debug("Renamed '{}' to '{}' (override file).", toRename.source, toRename.destination);
            } else {
                if (!this.context.fileSystem.rename(toRename.source, toRename.destination)) {
                    throw new FileAlreadyExistsException(toRename.destination.toString());
                }
                log.debug("Renamed '{}' to '{}'.", toRename.source, toRename.destination);
            }

            FileDescriptor newFile = toDescriptor(this.context.fileSystem.getFileStatus(toRename.destination));
            if (newFile.getOffset() != lastFile.getLastOffset() || newFile.getEpoch() != this.context.epoch) {
                throw new SegmentFilesCorruptedException(targetHandle.getSegmentName(), newFile,
                        String.format("Rename operation failed. Renamed file has unexpected parameters. Offset=%d/%d, Epoch=%d/%d.",
                                newFile.getOffset(), lastFile.getLastOffset(), newFile.getEpoch(), this.context.epoch));
            }

            // Rename successful. Cleanup and update the handle.
            removeConcatNext(lastFile);
            if (toRename.replace) {
                targetHandle.replaceLastFile(newFile);
            } else {
                targetHandle.addLastFile(newFile);
            }
        }

        LoggerHelpers.traceLeave(log, "performRenames", traceId, targetHandle, renames);
    }

    /**
     * Ensures the last file in the handle is read-only, unsealed (since the concat source was sealed).
     */
    private void cleanup(HDFSSegmentHandle targetHandle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "cleanup", targetHandle);
        FileDescriptor lastFile = targetHandle.getLastFile();
        makeReadOnly(lastFile);
        makeUnsealed(lastFile);
        removeConcatNext(lastFile);

        // Create new empty file (read-write).
        Path newActiveFile = getFilePath(targetHandle.getSegmentName(), lastFile.getLastOffset(), this.context.epoch);
        createEmptyFile(newActiveFile);
        targetHandle.addLastFile(toDescriptor(this.context.fileSystem.getFileStatus(newActiveFile)));
        LoggerHelpers.traceLeave(log, "cleanup", traceId, targetHandle);
    }

    @RequiredArgsConstructor
    private static class RenamePair {
        private final Path source;
        private final Path destination;
        private final boolean replace;

        @Override
        public String toString() {
            return String.format("%s: From '%s' to '%s'.", replace ? "Replace" : "Rename", source, destination);
        }
    }
}
