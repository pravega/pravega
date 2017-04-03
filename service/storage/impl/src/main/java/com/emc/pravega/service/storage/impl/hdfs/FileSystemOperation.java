/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Base for any Operation that accesses the FileSystem.
 */
@Slf4j
abstract class FileSystemOperation<T> {
    //region Members

    static final String PART_SEPARATOR = "_";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s" + PART_SEPARATOR + "%s";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<offset>", "<epoch>");
    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final String SEALED_ATTRIBUTE = "user.sealed";
    static final String CONCAT_ATTRIBUTE = "user.concat.next";
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    @Getter
    protected final T target;
    protected final OperationContext context;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileSystemOperation class.
     *
     * @param target  Target of this Operation. Usually a Segment Name or SegmentHandle.
     * @param context Context for this operation.
     */
    FileSystemOperation(T target, OperationContext context) {
        Preconditions.checkNotNull(target, "target");
        this.target = target;
        this.context = context;
    }

    //endregion

    //region File Organization and Fencing

    /**
     * Gets an array (not necessarily ordered) of FileStatus objects currently available for the given Segment.
     * These must be in the format specified by NAME_FORMAT (see EXAMPLE_NAME_FORMAT).
     */
    FileStatus[] findAllRaw(String segmentName) throws IOException {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        String pattern = String.format(NAME_FORMAT, getPathPrefix(segmentName), NUMBER_GLOB_REGEX, NUMBER_GLOB_REGEX);
        return this.context.fileSystem.globStatus(new Path(pattern));
    }

    /**
     * Gets an ordered list of FileDescriptor currently available for the given Segment, and validates that they are consistent.
     *
     * @param segmentName      The name of the Segment to retrieve for.
     * @param enforceExistence If true, it will throw a FileNotFoundException if no files are found, otherwise an empty
     *                         list is returned.
     * @return A List of FileDescriptor
     * @throws IOException If an exception occurred.
     */
    List<FileDescriptor> findAll(String segmentName, boolean enforceExistence) throws IOException {
        FileStatus[] rawFiles = findAllRaw(segmentName);
        if (rawFiles == null || rawFiles.length == 0) {
            if (enforceExistence) {
                throw HDFSExceptionHelpers.segmentNotExistsException(segmentName);
            }

            return Collections.emptyList();
        }

        val result = Arrays.stream(rawFiles)
                           .map(this::toDescriptor)
                           .sorted()
                           .collect(Collectors.toList());

        val firstFile = result.get(0);
        if (firstFile.getOffset() != 0 && isConcatSource(firstFile)) {
            // If the first file does not have offset 0 and it looks like it was part of an unfinished concat, then, by
            // convention, this segment does not exist. The Handle recovery for the target segment should finish up the job.
            if (enforceExistence) {
                throw HDFSExceptionHelpers.segmentNotExistsException(segmentName);
            }

            return Collections.emptyList();
        }

        // Validate the names are consistent with the file lengths.
        long expectedOffset = 0;
        for (FileDescriptor fi : result) {
            if (fi.getOffset() != expectedOffset) {
                throw new SegmentFilesCorruptedException(segmentName, fi,
                        String.format("Declared offset is '%d' but should be '%d'.", fi.getOffset(), expectedOffset));
            }

            expectedOffset += fi.getLength();
        }

        return result;
    }

    /**
     * Converts the given FileStatus into a FileDescriptor.
     */
    @SneakyThrows(FileNameFormatException.class)
    FileDescriptor toDescriptor(FileStatus fs) {
        // Extract offset and epoch from name.
        final long offset;
        final long epoch;
        String fileName = fs.getPath().getName();

        // We read backwards, because the segment name itself may have multiple PartSeparators in it, but we only care
        // about the last ones.
        int pos2 = fileName.lastIndexOf(PART_SEPARATOR);
        if (pos2 <= 0 || pos2 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        int pos1 = fileName.lastIndexOf(PART_SEPARATOR, pos2 - 1);
        if (pos1 <= 0 || pos1 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        try {
            offset = Long.parseLong(fileName.substring(pos1 + 1, pos2));
            epoch = Long.parseLong(fileName.substring(pos2 + 1));
        } catch (NumberFormatException nfe) {
            throw new FileNameFormatException(fileName, "Could not extract offset or epoch.", nfe);
        }

        return new FileDescriptor(fs.getPath(), offset, fs.getLen(), epoch, isReadOnly(fs));
    }

    /**
     * Verifies that the current segment has not been fenced out by another instance.
     *
     * @param segmentName       The name of the segment.
     * @param expectedFileCount The expected number of files in the file system. -1 means ignore.
     * @param lastFile          The last known file for this segment. This one's epoch will be compared against the files
     *                          currently in the file system.
     * @throws IOException                If a general exception occurred.
     * @throws StorageNotPrimaryException If this segment has been fenced out, using the arguments supplied above.
     */
    List<FileDescriptor> checkForFenceOut(String segmentName, int expectedFileCount, FileDescriptor lastFile) throws IOException, StorageNotPrimaryException {
        val systemFiles = findAll(segmentName, true);
        if (expectedFileCount >= 0 && systemFiles.size() != expectedFileCount) {
            // The files were changed externally (files removed or added). We cannot continue.
            throw new StorageNotPrimaryException(segmentName,
                    String.format("File count in FileSystem (%d) is different than the expected value (%d).",
                            systemFiles.size(), expectedFileCount));
        }

        val lastSystemFile = systemFiles.get(systemFiles.size() - 1);
        if (lastSystemFile.getEpoch() > lastFile.getEpoch()) {
            // The last file's epoch in the file system is higher than ours. We have been fenced out.
            throw new StorageNotPrimaryException(segmentName,
                    String.format("Last file in FileSystem (%s) has a higher epoch than that of ours (%s).",
                            lastSystemFile, lastFile));
        }

        return systemFiles;
    }

    /**
     * Creates a new file with given path having a read-write permission.
     *
     * @param fullPath The path of the file to create.
     * @throws IOException If an exception occurred.
     */
    void createEmptyFile(Path fullPath) throws IOException {
        this.context.fileSystem
                .create(fullPath,
                        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                        false,
                        0,
                        this.context.config.getReplication(),
                        this.context.config.getBlockSize(),
                        null)
                .close();
        log.debug("Created '{}'.", fullPath);
    }

    /**
     * Gets the full HDFS Path to a file for the given Segment, startOffset and epoch.
     */
    Path getFilePath(String segmentName, long startOffset, long epoch) {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        assert startOffset >= 0 : "startOffset must be non-negative " + startOffset;
        assert epoch >= 0 : "epoch must be non-negative " + epoch;
        return new Path(String.format(NAME_FORMAT, getPathPrefix(segmentName), startOffset, epoch));
    }

    /**
     * Gets an HDFS-friendly path prefix for the given Segment name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String segmentName) {
        return this.context.config.getHdfsRoot() + Path.SEPARATOR + segmentName;
    }

    //endregion

    //region File Attributes

    /**
     * Deletes a file from the file system.
     *
     * @param file The path of the file to delete.
     * @throws IOException If an exception occurred.
     */
    void deleteFile(FileDescriptor file) throws IOException {
        this.context.fileSystem.delete(file.getPath(), true);
        log.debug("Deleted '{}'.", file.getPath());
    }

    /**
     * Determines whether the file represented by the given FileDescriptor has the Sealed attribute set.
     *
     * @param file The FileDescriptor of the file toe make sealed.
     * @return True or False.
     * @throws IOException If an exception occurred.
     */
    boolean isSealed(FileDescriptor file) throws IOException {
        byte[] data = this.context.fileSystem.getXAttr(file.getPath(), SEALED_ATTRIBUTE);
        return data != null && data.length > 0 && data[0] != 0;
    }

    /**
     * Sets the Sealed attribute on the file represented by the given descriptor.
     *
     * @param file The FileDescriptor of the file to make sealed.
     * @throws IOException If an exception occurred.
     */
    void makeSealed(FileDescriptor file) throws IOException {
        this.context.fileSystem.setXAttr(file.getPath(), SEALED_ATTRIBUTE, new byte[]{(byte) (255)});
        log.debug("MakeSealed '{}'.", file.getPath());
    }

    /**
     * Removes the sealed attribute from the file represented by the given descriptor.
     *
     * @param file The FileDescriptor of the file to unseal.
     * @throws IOException If an exception occurred.
     */
    void makeUnsealed(FileDescriptor file) throws IOException {
        this.context.fileSystem.removeXAttr(file.getPath(), SEALED_ATTRIBUTE);
        log.debug("MakeUnsealed '{}'.", file.getPath());
    }

    /**
     * Determines whether the given FileStatus indicates the file is read-only.
     *
     * @param fs The FileStatus to check.
     * @return True or false.
     */
    boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    /**
     * Makes the file represented by the given FileDescriptor read-only.
     *
     * @param file The FileDescriptor of the file to set. If this method returns true, this FileDescriptor will
     *             also be updated to indicate the file is read-only.
     * @return True if the file was not read-only before (and it is now), or false if the file was already read-only.
     * @throws IOException If an exception occurred.
     */
    boolean makeReadOnly(FileDescriptor file) throws IOException {
        if (isReadOnly(this.context.fileSystem.getFileStatus(file.getPath()))) {
            return false;
        }

        this.context.fileSystem.setPermission(file.getPath(), READONLY_PERMISSION);
        log.debug("MakeReadOnly '{}'.", file.getPath());
        file.markReadOnly();
        return true;
    }

    /**
     * Sets an attribute on the given file to indicate which file is next in the concatenation order.
     *
     * @param file     The FileDescriptor of the file to set the attribute on.
     * @param nextFile The FileDescriptor of the file to point the attribute to.
     * @throws IOException If an exception occurred.
     */
    void setConcatNext(FileDescriptor file, FileDescriptor nextFile) throws IOException {
        this.context.fileSystem.setXAttr(file.getPath(), CONCAT_ATTRIBUTE, nextFile.getPath().toString().getBytes());
        log.debug("SetConcatNext '{}' to '{}'.", file.getPath(), nextFile.getPath());
    }

    /**
     * Gets the value of the concatNext attribute on the given file.
     *
     * @param file The FileDescriptor of the file to get the attribute for.
     * @return The value of the attribute, or null if no such attribute is set.
     * @throws IOException If an exception occurred.
     */
    String getConcatNext(FileDescriptor file) throws IOException {
        byte[] data = this.context.fileSystem.getXAttr(file.getPath(), CONCAT_ATTRIBUTE);
        if (data == null || data.length == 0) {
            return null;
        }

        return new String(data);
    }

    /**
     * Removes the concatNext attribute from the given file.
     *
     * @param file The FileDescriptor of the file to remove the attribute from.
     * @throws IOException If an exception occurred.
     */
    void removeConcatNext(FileDescriptor file) throws IOException {
        this.context.fileSystem.removeXAttr(file.getPath(), CONCAT_ATTRIBUTE);
        log.debug("RemoveConcatNext '{}' to '{}'.", file.getPath());
    }

    /**
     * Gets a value indicating whether the concatNext attribute is present - this indicates that this file is part of a
     * segment that was chosen as a source of concatenation.
     *
     * @param fileDescriptor The FileDescriptor of the file to check.
     * @return True if the attribute is set, false otherwise.
     * @throws IOException If an exception occurred.
     */
    boolean isConcatSource(FileDescriptor fileDescriptor) throws IOException {
        return getConcatNext(fileDescriptor) != null;
    }

    //endregion

    //region OperationContext

    /**
     * Context for each operation.
     */
    @RequiredArgsConstructor
    static class OperationContext {
        final long epoch;
        final FileSystem fileSystem;
        final HDFSStorageConfig config;
    }

    //endregion
}
