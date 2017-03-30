/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

    private static final String SEPARATOR = "_";
    private static final String NAME_FORMAT = "%s" + SEPARATOR + "%s" + SEPARATOR + "%s";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<offset>", "<epoch>");
    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final String SEALED_ATTRIBUTE = "user.sealed";
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    @Getter
    protected final T target;
    protected final OperationContext context;

    //endregion

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

    protected void checkForFenceOut(HDFSSegmentHandle handle) throws IOException, StorageNotPrimaryException {
        checkForFenceOut(handle.getSegmentName(), handle.getFiles().size(), handle.getLastFile());
    }

    /**
     * TODO: write description. This should be the common protocol for fence-out detection used by all modify operations.
     *
     * @param segmentName
     * @param expectedFileCount
     * @param lastFile
     * @throws IOException
     * @throws StorageNotPrimaryException
     */
    protected void checkForFenceOut(String segmentName, int expectedFileCount, FileInfo lastFile) throws IOException, StorageNotPrimaryException {
        val systemFiles = findAll(segmentName, true);
        if (systemFiles.size() != expectedFileCount) {
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
     * Gets an ordered list of FileInfo currently available for the given Segment, and validates that they are consistent.
     *
     * @param segmentName      The name of the Segment to retrieve for.
     * @param enforceExistence If true, it will throw a FileNotFoundException if no files are found, otherwise an empty
     *                         list is returned.
     * @return A List of FileInfo
     * @throws IOException If an exception occurred.
     */
    protected List<FileInfo> findAll(String segmentName, boolean enforceExistence) throws IOException {
        FileStatus[] rawFiles = findAllRaw(segmentName);
        if (rawFiles == null) {
            if (enforceExistence) {
                throw new FileNotFoundException(segmentName);
            }
            return Collections.emptyList();
        }

        val result = Arrays.stream(rawFiles)
                           .map(this::toFileInfo)
                           .sorted(Comparator.comparingLong(FileInfo::getOffset))
                           .collect(Collectors.toList());

        // Validate the names are consistent with the file lengths.
        long expectedOffset = 0;
        for (FileInfo fi : result) {
            if (fi.getOffset() != expectedOffset) {
                // TODO: better exception.
                throw new IOException(String.format("Segment '%s' has invalid files. File '%s' declares offset '%d' but should be '%d'.",
                        segmentName, fi.getPath(), fi.getOffset(), expectedOffset));
            }

            expectedOffset += fi.getLength();
        }

        return result;
    }

    /**
     * Converts the given FileStatus into a FileInfo.
     */
    @SneakyThrows(FileNameFormatException.class)
    private FileInfo toFileInfo(FileStatus fs) {
        // Extract offset and epoch from name.
        final long offset;
        final long epoch;
        String fileName = fs.getPath().getName();
        int pos1 = fileName.indexOf(SEPARATOR);
        if (pos1 <= 0 || pos1 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        int pos2 = fileName.indexOf(SEPARATOR, pos1 + 1);
        if (pos2 <= 0 || pos2 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        try {
            offset = Long.parseLong(fileName.substring(pos1 + 1, pos2));
            epoch = Long.parseLong(fileName.substring(pos2 + 1));
        } catch (NumberFormatException nfe) {
            throw new FileNameFormatException(fileName, "Could not extract offset or epoch.", nfe);
        }

        return new FileInfo(fs.getPath().toString(), offset, fs.getLen(), epoch, isReadOnly(fs));
    }

    protected boolean isSealed(FileInfo fileInfo) throws IOException {
        byte[] data = this.context.fileSystem.getXAttr(new Path(fileInfo.getPath()), SEALED_ATTRIBUTE);
        return data != null && data.length > 0 && data[0] != 0;
    }

    protected void makeSealed(FileInfo fileInfo) throws IOException {
        this.context.fileSystem.setXAttr(new Path(fileInfo.getPath()), SEALED_ATTRIBUTE, new byte[]{(byte) 255});
    }

    protected boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    protected boolean makeReadOnly(FileInfo fileInfo) throws IOException {
        Path p = new Path(fileInfo.getPath());
        if (isReadOnly(this.context.fileSystem.getFileStatus(p))) {
            return false;
        }

        this.context.fileSystem.setPermission(p, READONLY_PERMISSION);
        fileInfo.markReadOnly();
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
