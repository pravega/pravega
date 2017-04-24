/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
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
    static final String CONCAT_ATTRIBUTE = "user.concat.next";
    private static final String SEALED_ATTRIBUTE = "user.sealed";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s" + PART_SEPARATOR + "%s";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<offset>", "<epoch>");
    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
    private static final Charset ATTRIBUTE_CHARSET = Charset.forName("UTF-8");
    private static final byte[] ATTRIBUTE_VALUE_TRUE = new byte[]{(byte) 255};
    private static final byte[] ATTRIBUTE_VALUE_FALSE = new byte[]{(byte) 0};

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
     * Gets an ordered list of FileDescriptors currently available for the given Segment, and validates that they are consistent.
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
                           .sorted(this::compareFileDescriptors)
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
     * @param path The path of the file to create.
     * @throws IOException If an exception occurred.
     */
    void createEmptyFile(Path path) throws IOException {
        this.context.fileSystem
                .create(path,
                        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                        false,
                        0,
                        this.context.config.getReplication(),
                        this.context.config.getBlockSize(),
                        null)
                .close();
        setBooleanAttributeValue(path, SEALED_ATTRIBUTE, false);
        log.debug("Created '{}'.", path);
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

    private int compareFileDescriptors(FileDescriptor f1, FileDescriptor f2) {
        int diff = Long.compare(f1.getOffset(), f2.getOffset());
        if (diff == 0) {
            diff = Long.compare(f1.getEpoch(), f2.getEpoch());
        }

        return diff;
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
        return getBooleanAttributeValue(file.getPath(), SEALED_ATTRIBUTE);
    }

    /**
     * Sets the Sealed attribute on the file represented by the given descriptor.
     *
     * @param file The FileDescriptor of the file to make sealed.
     * @throws IOException If an exception occurred.
     */
    void makeSealed(FileDescriptor file) throws IOException {
        setBooleanAttributeValue(file.getPath(), SEALED_ATTRIBUTE, true);
        log.debug("MakeSealed '{}'.", file.getPath());
    }

    /**
     * Updates the sealed attribute on the file represented by the given descriptor to indicate it is not sealed.
     *
     * @param file The FileDescriptor of the file to unseal.
     * @throws IOException If an exception occurred.
     */
    void makeUnsealed(FileDescriptor file) throws IOException {
        setBooleanAttributeValue(file.getPath(), SEALED_ATTRIBUTE, false);
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
        this.context.fileSystem.setXAttr(file.getPath(), CONCAT_ATTRIBUTE, nextFile.getPath().toString().getBytes(ATTRIBUTE_CHARSET));
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
        byte[] data = getAttributeValue(file.getPath(), CONCAT_ATTRIBUTE);
        if (data == null || data.length == 0) {
            return null;
        }

        return new String(data, ATTRIBUTE_CHARSET);
    }

    /**
     * Removes the concatNext attribute from the given file.
     *
     * @param file The FileDescriptor of the file to remove the attribute from.
     * @throws IOException If an exception occurred.
     */
    void removeConcatNext(FileDescriptor file) throws IOException {
        removeAttribute(file, CONCAT_ATTRIBUTE);
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

    private void setBooleanAttributeValue(Path path, String attributeName, boolean value) throws IOException {
        this.context.fileSystem.setXAttr(path, attributeName, value ? ATTRIBUTE_VALUE_TRUE : ATTRIBUTE_VALUE_FALSE);
    }

    private boolean getBooleanAttributeValue(Path path, String attributeName) throws IOException {
        byte[] data = getAttributeValue(path, attributeName);
        return data != null && data.length > 0 && data[0] != 0;
    }

    private byte[] getAttributeValue(Path path, String attributeName) throws FileNotFoundException {
        try {
            return this.context.fileSystem.getXAttr(path, attributeName);
        } catch (FileNotFoundException fnf) {
            throw fnf;
        } catch (IOException ex) {
            // It turns out that the getXAttr() implementation in 'org.apache.hadoop.hdfs.DistributedFileSystem' throws a
            // generic IOException if the attribute is not found. Since there's no specific exception or flag to filter
            // this out, we're going to treat all IOExceptions (except FileNotFoundExceptions) as "attribute is not set".
            return null;
        }
    }

    private void removeAttribute(FileDescriptor file, String attributeName) throws FileNotFoundException {
        try {
            this.context.fileSystem.removeXAttr(file.getPath(), attributeName);
        } catch (FileNotFoundException fnf) {
            throw fnf;
        } catch (IOException ex) {
            // See getAttributeValue for explanation.
        }
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
