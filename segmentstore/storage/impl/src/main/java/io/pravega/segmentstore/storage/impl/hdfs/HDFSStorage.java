/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

/**
 * Storage adapter for a backing HDFS Store which implements fencing using file-chaining strategy.
 * <p>
 * Each segment is represented by one file, adopting the following pattern: {segment-name}_{epoch}.
 * <ul>
 * <li> {segment-name} is the name of the segment as used in the SegmentStore
 * <li> {epoch} is the Container Epoch which had ownership of that segment when that file was created.
 * </ul>
 * <p>
 * Example: Segment "foo" can have these files
 * <ol>
 * <li> foo_<epoc>: Segement file, owned by a segmentstore running under epoch 1.
 * <li> foo_<MAX_LONG>: A sealed segment.
 * <p>
 * When a container fails over and needs to reacquire ownership of a segment, it renames the segment file as foo_<current_epoc>.
 * After creation of the file, the filename is checked again. If there exists any file with higher epoc, the current file is deleted
 * and access is ceded to the later owner.
 * <p>
 * When a failover happens, the previous Container (if still active) will detect that its file is not present and is renamed to
 * a file with higher epoch and know it's time to stop all activity for that segment (i.e., it was fenced out).
 * <p>
 */
@Slf4j
class HDFSStorage implements SyncStorage {
    private static final String PART_SEPARATOR = "_";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s";
    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<epoch>");
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
    private static final int MAX_ATTEMPT_COUNT = 3;
    private static final long MAX_EPOC = Long.MAX_VALUE;
    //region Members

    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private OperationContext context;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     */
    HDFSStorage(HDFSStorageConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.context != null) {
                try {
                    this.context.fileSystem.close();
                    this.context = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem: {}.", e);
                }
            }
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    @SneakyThrows(IOException.class)
    public void initialize(long epoch) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.context == null, "HDFSStorage has already been initialized.");
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number. Given %s.", epoch);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", this.config.getHdfsHostURL());
        conf.set("fs.default.fs", this.config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // FileSystem has a bad habit of caching Clients/Instances based on target URI. We do not like this, since we
        // want to own our implementation so that when we close it, we don't interfere with others.
        conf.set("fs.hdfs.impl.disable.cache", "true");
        if (!this.config.isReplaceDataNodesOnFailure()) {
            // Default is DEFAULT, so we only set this if we want it disabled.
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        }

        this.context = new OperationContext(epoch, openFileSystem(conf), this.config);
        log.info("Initialized (HDFSHost = '{}', Epoch = {}).", this.config.getHdfsHostURL(), epoch);
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        SegmentProperties result = null;
        int attemptCount = 0;
        do {
            long length = 0;
            boolean isSealed = false;
            FileStatus last = null;
            try {
                last = findStatusForSegment(streamSegmentName, true);
                isSealed = isSealed(last.getPath());
            } catch (IOException fnf) {
                // This can happen if we get a concurrent call to SealOperation with an empty last file; the last file will
                // be deleted in that case so we need to try our luck again (in which case we need to refresh the file list).
                if (++attemptCount < MAX_ATTEMPT_COUNT) {
                    continue;
                }
                HDFSExceptionHelpers.throwException(streamSegmentName, fnf);
            }

            result = StreamSegmentInformation.builder().name(streamSegmentName).length(last.getLen()).sealed(isSealed).build();
        } while (result == null);
        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName, result);
        return result;
    }

    @Override
    public boolean exists(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
        FileStatus status;
        try {
            status = findStatusForSegment(streamSegmentName, false);
        } catch (IOException e) {
            return false;
        }
        boolean exists = status != null;
        LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName, exists);
        return exists;
    }

    private boolean isSealed(Path path) throws FileNameFormatException {
        return getEpocFromPath(path) == MAX_EPOC;
    }

    protected FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        Timer timer = new Timer();

        int attemptCount = 0;
        AtomicInteger totalBytesRead = new AtomicInteger();
        boolean needsRefresh = true;
        while (needsRefresh && attemptCount < MAX_ATTEMPT_COUNT) {
            attemptCount++;
            // Read data.
            try {
                readInternal(handle, totalBytesRead, buffer, offset, bufferOffset, length);
            } catch (IOException e) {
                HDFSExceptionHelpers.throwException(handle.getSegmentName(), e);
            }
            needsRefresh = false;
        }

        HDFSMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
        HDFSMetrics.READ_BYTES.add(totalBytesRead.get());
        LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
        return totalBytesRead.get();
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        try {
            //Ensure that file exists
            findStatusForSegment(streamSegmentName, true);
            return HDFSSegmentHandle.read(streamSegmentName, null);
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(streamSegmentName, e);
            return null;
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        handle = asWritableHandle(handle);
        try {
            FileStatus lastHandleFile = findStatusForSegment(handle.getSegmentName(), true);

            if (!isSealed(lastHandleFile.getPath())) {
                if (getEpoc(lastHandleFile) > this.context.epoch) {
                    throw new StorageNotPrimaryException(handle.getSegmentName());
                }
                makeReadOnly(lastHandleFile);
                Path sealedPath = getSealedFilePath(handle.getSegmentName());
                this.context.fileSystem.rename(lastHandleFile.getPath(), sealedPath);
            }
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(handle.getSegmentName(), e);
        }

        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        try {
            FileStatus status = findStatusForSegment(handle.getSegmentName(), true);
            if (!isSealed(status.getPath())) {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
            makeWrite(status);
            this.context.fileSystem.rename(status.getPath(), getFilePath(handle.getSegmentName(), this.context.epoch));
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(handle.getSegmentName(), e);
        }
    }

    @Override
    public void concat(SegmentHandle target, long offset, String sourceSegment) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, offset, sourceSegment);

        target = asWritableHandle(target);
        // Check for target offset and whether it is sealed.
        FileStatus lastFile = null;
        try {
            lastFile = findStatusForSegment(target.getSegmentName(), true);

            if (isSealed(lastFile.getPath())) {
                throw new StreamSegmentSealedException(target.getSegmentName());
            } else if (getEpoc(lastFile) > this.context.epoch) {
                throw new StorageNotPrimaryException(target.getSegmentName());
            } else if (lastFile.getLen() != offset) {
                throw new BadOffsetException(target.getSegmentName(), lastFile.getLen(), offset);
            }

            FileStatus sourceFile;
            sourceFile = findStatusForSegment(sourceSegment, true);
            Preconditions.checkState(isSealed(sourceFile.getPath()),
                    "Cannot concat segment '%s' into '%s' because it is not sealed.", sourceSegment, target.getSegmentName());

            // Concat source files into target and update the handle.
            makeWrite(sourceFile);
            this.context.fileSystem.concat(lastFile.getPath(), new Path[]{sourceFile.getPath()});
            this.context.fileSystem.delete(sourceFile.getPath(), true);
        } catch (IOException ex) {
            HDFSExceptionHelpers.throwException(sourceSegment, ex);
        }

        LoggerHelpers.traceLeave(log, "concat", traceId, target, offset, sourceSegment);
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
        handle = asWritableHandle(handle);
        try {
            // Get an initial list of all files.
            FileStatus files = findStatusForSegment(handle.getSegmentName(), true);
            if (getEpoc(files) > this.context.epoch && !isSealed(files.getPath())) {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
            this.context.fileSystem.delete(files.getPath(), true);
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(handle.getSegmentName(), e);
        }
        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }


    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }



    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
        handle = asWritableHandle(handle);
        FileStatus lastFile = null;
        try {
            lastFile = findStatusForSegment(handle.getSegmentName(), true);
            if (isSealed(lastFile.getPath())) {
                throw new StreamSegmentSealedException(handle.getSegmentName());
            }
            if (getEpocFromPath(lastFile.getPath()) > this.context.epoch) {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(handle.getSegmentName(), e);
        }

        Timer timer = new Timer();
        try (FSDataOutputStream stream = this.context.fileSystem.append(lastFile.getPath())) {
            if (offset != lastFile.getLen()) {
                // Do the handle offset validation here, after we open the file. We want to throw FileNotFoundException
                // before we throw BadOffsetException.
                throw new BadOffsetException(handle.getSegmentName(), lastFile.getLen(), offset);
            } else if (stream.getPos() != offset) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                // Update our knowledge of the filesystem and throw a BadOffsetException - this should cause upstream code
                // to try to reconcile; if it can't then the upstream code should shut down or take other appropriate measures.
                log.warn("File changed detected for '{}'. Expected length = {}, actual length = {}.", lastFile, lastFile.getLen(), stream.getPos());
                throw new BadOffsetException(handle.getSegmentName(), lastFile.getLen(), offset);
            }

            if (length == 0) {
                // Exit here (vs at the beginning of the method), since we want to throw appropriate exceptions in case
                // of Sealed or BadOffset
                // Note: IOUtils.copyBytes with length == 0 will enter an infinite loop, hence the need for this check.
                return;
            }

            // We need to be very careful with IOUtils.copyBytes. There are many overloads with very similar signatures.
            // There is a difference between (InputStream, OutputStream, int, boolean) and (InputStream, OutputStream, long, boolean),
            // in that the one with "int" uses the third arg as a buffer size, and the one with "long" uses it as the number
            // of bytes to copy.
            IOUtils.copyBytes(data, stream, (long) length, false);

            stream.flush();
        } catch (IOException ex) {
            HDFSExceptionHelpers.throwException(handle.getSegmentName(), ex);
        }

        HDFSMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        HDFSMetrics.WRITE_BYTES.add(length);
        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, length);
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        int fencedCount = 0;
        do {
            try {
                FileStatus existingFiles = findStatusForSegment(streamSegmentName, true);

                if (!isSealed(existingFiles.getPath())) {
                    if (getEpocFromPath(existingFiles.getPath()) > this.context.epoch) {
                        throw new StorageNotPrimaryException(streamSegmentName);
                    }

                    Path targetPath = getFilePath(streamSegmentName, this.context.epoch);
                    if (!targetPath.equals(existingFiles.getPath())) {
                        try {
                            this.context.fileSystem.rename(existingFiles.getPath(), targetPath);
                        } catch (FileNotFoundException e) {
                            //This happens when more than one host is trying to fence and only one of the host goes through
                            // retry the rename so that host with the highest epoch gets access.
                            log.warn("Race in fencing. More than two hosts trying to own the segment. Retrying");
                            fencedCount++;
                            continue;
                        }
                    }
                }
                //Ensure that file exists
                findStatusForSegment(streamSegmentName, true);
                return HDFSSegmentHandle.write(streamSegmentName, null);
            } catch (IOException e) {
                HDFSExceptionHelpers.throwException(streamSegmentName, e);
                return null;
            }
        } while (fencedCount < MAX_ATTEMPT_COUNT);
        return null;
    }

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
        // Create the segment using our own epoch.
        FileStatus[] existingFiles = null;
        try {
            existingFiles = findAllRaw(streamSegmentName);
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(streamSegmentName, e);
        }
        if (existingFiles != null && existingFiles.length > 0) {
            // Segment already exists; don't bother with anything else.
            HDFSExceptionHelpers.throwException(streamSegmentName, HDFSExceptionHelpers.segmentExistsException(streamSegmentName));
        }

        // Create the first file in the segment.
        Path fullPath = getFilePath(streamSegmentName, this.context.epoch);
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, fullPath);
        try {
            // Create the file, and then immediately close the returned OutputStream, so that HDFS may properly create the file.
            this.context.fileSystem.create(fullPath, READWRITE_PERMISSION, false, 0, this.context.config.getReplication(),
                    this.context.config.getBlockSize(), null).close();
            log.debug("Created '{}'.", fullPath);
        } catch (IOException e) {
            HDFSExceptionHelpers.throwException(streamSegmentName, e);
        }
        LoggerHelpers.traceLeave(log, "create", traceId, streamSegmentName);
        return StreamSegmentInformation.builder().name(streamSegmentName).build();
    }
    //endregion

    //region Helpers

    /**
     * Casts the given handle as a HDFSSegmentHandle that has isReadOnly == false.
     */
    private HDFSSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    /**
     * Casts the given handle as a HDFSSegmentHandle irrespective of its isReadOnly value.
     */
    private HDFSSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof HDFSSegmentHandle, "handle must be of type HDFSSegmentHandle.");
        return (HDFSSegmentHandle) handle;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.context != null, "HDFSStorage is not initialized.");
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

    //Region HDFS helper methods.

    /**
     * Gets an array (not necessarily ordered) of FileStatus objects currently available for the given Segment.
     * These must be in the format specified by NAME_FORMAT (see EXAMPLE_NAME_FORMAT).
     */
    FileStatus[] findAllRaw(String segmentName) throws IOException {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        String pattern = String.format(NAME_FORMAT, getPathPrefix(segmentName), NUMBER_GLOB_REGEX);
        FileStatus[] files = this.context.fileSystem.globStatus(new Path(pattern));

        if (files.length > 1) {
            throw new IllegalArgumentException("More than one file");
        }
        return files;
    }

    /**
     * Gets an HDFS-friendly path prefix for the given Segment name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String segmentName) {
        return this.context.config.getHdfsRoot() + Path.SEPARATOR + segmentName;
    }

    /**
     * Gets the full HDFS Path to a file for the given Segment, startOffset and epoch.
     */
    Path getFilePath(String segmentName, long epoch) {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        assert epoch >= 0 : "epoch must be non-negative " + epoch;
        return new Path(String.format(NAME_FORMAT, getPathPrefix(segmentName), epoch));
    }

    /**
     * Gets the full HDFS path when sealed
     */
    Path getSealedFilePath(String segmentName) {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        return new Path(String.format(NAME_FORMAT, getPathPrefix(segmentName), MAX_EPOC));
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
    private FileStatus findStatusForSegment(String segmentName, boolean enforceExistence) throws IOException {
        FileStatus[] rawFiles = findAllRaw(segmentName);
        if (rawFiles == null || rawFiles.length == 0) {
            if (enforceExistence) {
                throw HDFSExceptionHelpers.segmentNotExistsException(segmentName);
            }

            return null;
        }

        val result = Arrays.stream(rawFiles)
                           .sorted(this::compareFileDescriptors)
                           .collect(Collectors.toList());
        return result.get(result.size() -1);
    }

    @SneakyThrows(FileNameFormatException.class)
    private int compareFileDescriptors(FileStatus f1, FileStatus f2) {
        return Long.compare(getEpoc(f1), getEpoc(f2));
    }

    private long getEpoc(FileStatus status) throws FileNameFormatException {
        return getEpocFromPath(status.getPath());
    }

    private long getEpocFromPath(Path path) throws FileNameFormatException {
        String fileName = path.toString();
        int pos2 = fileName.lastIndexOf(PART_SEPARATOR);
        if (pos2 <= 0) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }
        if ( pos2 == fileName.length() - 1) {
            //File is sealed. This is the final version
            return MAX_EPOC;
        }
        try {
            return Long.parseLong(fileName.substring(pos2 + 1));
        } catch (NumberFormatException nfe) {
            throw new FileNameFormatException(fileName, "Could not extract offset or epoch.", nfe);
        }
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
    private boolean makeReadOnly(FileStatus file) throws IOException {
        if (isReadOnly(file)) {
            return false;
        }

        this.context.fileSystem.setPermission(file.getPath(), READONLY_PERMISSION);
        log.debug("MakeReadOnly '{}'.", file.getPath());
        return true;
    }

    private boolean makeWrite(FileStatus file) throws IOException {
        this.context.fileSystem.setPermission(file.getPath(), READWRITE_PERMISSION);
        log.debug("MakeReadOnly '{}'.", file.getPath());
        return true;
    }

    private void readInternal(SegmentHandle handle, AtomicInteger totalBytesRead, byte[] buffer, long offset, int bufferOffset, int length) throws IOException {
        //There is only one file per segment.
        FileStatus currentFile = findStatusForSegment(handle.getSegmentName(), true);
        try (FSDataInputStream stream = this.context.fileSystem.open(currentFile.getPath())) {
            if (offset + length > stream.available()) {
                throw new ArrayIndexOutOfBoundsException();
            }
            stream.readFully(offset, buffer, bufferOffset + totalBytesRead.get(), length);
            totalBytesRead.addAndGet(length);
        } catch (EOFException ex) {
            throw new IOException(
                    String.format("Internal error while reading segment file. Attempted to read file '%s' at offset %d, length %d.",
                            currentFile, offset, length),
                    ex);
        }

    }


    //endregion
}
