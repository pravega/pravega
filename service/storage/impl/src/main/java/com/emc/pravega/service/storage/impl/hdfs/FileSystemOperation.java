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
import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Base for any Operation that accesses the FileSystem.
 */
abstract class FileSystemOperation<T> {
    //region Members

    private static final String PART_SEPARATOR = "_";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s" + PART_SEPARATOR + "%s";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<offset>", "<epoch>");
    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final String SEALED_ATTRIBUTE = "user.sealed";
    static final String CONCAT_ATTRIBUTE = "user.concat";
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

    /**
     * TODO: write description. This should be the common protocol for fence-out detection used by all modify operations.
     *
     * @param segmentName
     * @param expectedFileCount
     * @param lastFile
     * @throws IOException
     * @throws StorageNotPrimaryException
     */
    protected List<FileDescriptor> checkForFenceOut(String segmentName, int expectedFileCount, FileDescriptor lastFile) throws IOException, StorageNotPrimaryException {
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

    protected void createEmptyFile(String fullPath) throws IOException {
        this.context.fileSystem
                .create(new Path(fullPath),
                        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                        false,
                        0,
                        this.context.config.getReplication(),
                        this.context.config.getBlockSize(),
                        null)
                .close();
    }

    protected void deleteFile(FileDescriptor file) throws IOException {
        this.context.fileSystem.delete(new Path(file.getPath()), true);
    }

    /**
     * Gets an array (not necessarily ordered) of FileStatus objects currently available for the given Segment.
     * These must be in the format specified by NAME_FORMAT (see EXAMPLE_NAME_FORMAT).
     */
    protected FileStatus[] findAllRaw(String segmentName) throws IOException {
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
    protected List<FileDescriptor> findAll(String segmentName, boolean enforceExistence) throws IOException {
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
    private FileDescriptor toDescriptor(FileStatus fs) {
        // Extract offset and epoch from name.
        final long offset;
        final long epoch;
        String fileName = fs.getPath().getName();
        int pos1 = fileName.indexOf(PART_SEPARATOR);
        if (pos1 <= 0 || pos1 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        int pos2 = fileName.indexOf(PART_SEPARATOR, pos1 + 1);
        if (pos2 <= 0 || pos2 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        try {
            offset = Long.parseLong(fileName.substring(pos1 + 1, pos2));
            epoch = Long.parseLong(fileName.substring(pos2 + 1));
        } catch (NumberFormatException nfe) {
            throw new FileNameFormatException(fileName, "Could not extract offset or epoch.", nfe);
        }

        return new FileDescriptor(fs.getPath().toString(), offset, fs.getLen(), epoch, isReadOnly(fs));
    }

    protected boolean isSealed(FileDescriptor fileDescriptor) throws IOException {
        return isBooleanAttributeSet(fileDescriptor, SEALED_ATTRIBUTE);
    }

    protected boolean isConcatSource(FileDescriptor fileDescriptor) throws IOException {
        return isBooleanAttributeSet(fileDescriptor, CONCAT_ATTRIBUTE);
    }

    private boolean isBooleanAttributeSet(FileDescriptor fileDescriptor, String attributeName) throws IOException {
        byte[] data = this.context.fileSystem.getXAttr(new Path(fileDescriptor.getPath()), attributeName);
        return data != null && data.length > 0 && data[0] != 0;
    }

    protected void makeSealed(FileDescriptor fileDescriptor) throws IOException {
        setBooleanAttribute(fileDescriptor, SEALED_ATTRIBUTE, true);
    }

    private void setBooleanAttribute(FileDescriptor fileDescriptor, String attributeName, boolean value) throws IOException {
        this.context.fileSystem.setXAttr(new Path(fileDescriptor.getPath()), attributeName, new byte[]{(byte) (value ? 255 : 0)});
    }

    protected boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    protected boolean makeReadOnly(FileDescriptor fileDescriptor) throws IOException {
        Path p = new Path(fileDescriptor.getPath());
        if (isReadOnly(this.context.fileSystem.getFileStatus(p))) {
            return false;
        }

        this.context.fileSystem.setPermission(p, READONLY_PERMISSION);
        fileDescriptor.markReadOnly();
        return true;
    }

    /**
     * Gets the full HDFS Path to a file for the given Segment, startOffset and epoch.
     */
    protected String getFileName(String segmentName, long startOffset, long epoch) {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        assert startOffset >= 0 : "startOffset must be non-negative " + startOffset;
        assert epoch >= 0 : "epoch must be non-negative " + epoch;
        return String.format(NAME_FORMAT, getPathPrefix(segmentName), startOffset, epoch);
    }

    /**
     * Gets an HDFS-friendly path prefix for the given Segment name by pre-pending the HDFS root from the config.
     */
    protected String getPathPrefix(String segmentName) {
        return this.context.config.getHdfsRoot() + Path.SEPARATOR + segmentName;
    }

    /**
     * Context for each operation.
     */
    @RequiredArgsConstructor
    static class OperationContext {
        final long epoch;
        final FileSystem fileSystem;
        final HDFSStorageConfig config;
    }
}
