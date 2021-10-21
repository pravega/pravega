/**
 * Copyright Pravega Authors.
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
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
 * For each segment, there is exactly one file in the file system, adopting the following pattern: {segment-name}_{epoch}.
 * <ul>
 * <li> {segment-name} is the name of the segment as used in the SegmentStore
 * <li> {epoch} is the Container Epoch which has ownership of that segment.
 * </ul>
 * <p>
 * Example: Segment "foo" can have one of these files
 * <ol>
 * <li> foo_<epoch>: Segment file, owned by a SegmentStore running under epoch "epoch".
 * <li> foo_sealed: A sealed segment.
 * <p>
 * When a container fails over and needs to reacquire ownership of a segment, it renames the segment file as foo_<current_epoch>.
 * After creation of the file, the filename is checked again. If there exists any file with higher epoch, the current file is deleted
 * and access is ceded to the owner with highest epoch.
 * <p>
 * When a fail over happens, the previous Container (if still active) will detect that its file is not present and is renamed to
 * a file with higher epoch and know it's time to stop all activity for that segment (i.e., it was fenced out).
 * <p>
 */
@Slf4j
class HDFSStorage implements SyncStorage {
    private static final String PART_SEPARATOR = "_";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s";
    private static final String SEALED = "sealed";
    private static final String SUFFIX_GLOB_REGEX = "{" + "[0-9]*" + "," + SEALED + "}";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<epoch>");
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
    private static final int MAX_ATTEMPT_COUNT = 3;
    private static final long MAX_EPOCH = Long.MAX_VALUE;

    private static final Retry.RetryAndThrowExceptionally<FileNotFoundException, IOException> HDFS_RETRY = Retry
            .withExpBackoff(1, 5, MAX_ATTEMPT_COUNT)
            .retryingOn(FileNotFoundException.class)
            .throwingOn(IOException.class);

    //region Members

    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private long epoch;
    private FileSystem fileSystem;

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
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.close();
                    this.fileSystem = null;
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
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
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

        this.epoch = epoch;
        this.fileSystem = openFileSystem(conf);
        log.info("Initialized (HDFSHost = '{}', Epoch = {}).", this.config.getHdfsHostURL(), epoch);
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        try {
            return HDFS_RETRY.run(() -> {
                FileStatus last = findStatusForSegment(streamSegmentName, true);
                boolean isSealed = isSealed(last.getPath());
                StreamSegmentInformation result = StreamSegmentInformation.builder().name(streamSegmentName).length(last.getLen()).sealed(isSealed).build();
                LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName, result);
                return result;
            });
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(streamSegmentName, e);
        } catch (RetriesExhaustedException e) {
            throw HDFSExceptionHelpers.convertException(streamSegmentName, e.getCause());
        }
    }

    @Override
    public boolean exists(String streamSegmentName) {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
        FileStatus status = null;
        try {
            status = findStatusForSegment(streamSegmentName, false);
        } catch (IOException e) {
            // HDFS could not find the file. Returning false.
            log.warn("Got exception checking if file exists", e);
        }
        boolean exists = status != null;
        LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName, exists);
        return exists;
    }

    private static boolean isSealed(Path path) throws FileNameFormatException {
        return getEpochFromPath(path) == MAX_EPOCH;
    }

    FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        Timer timer = new Timer();

        try {
            return HDFS_RETRY.run(() -> {
                int totalBytesRead = readInternal(handle, buffer, offset, bufferOffset, length);
                HDFSMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
                HDFSMetrics.READ_BYTES.add(totalBytesRead);
                LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
                return totalBytesRead;
            });
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e);
        } catch (RetriesExhaustedException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e.getCause());
        }
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
        try {
            //Ensure that file exists
            findStatusForSegment(streamSegmentName, true);
            LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
            return HDFSSegmentHandle.read(streamSegmentName);
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(streamSegmentName, e);
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        handle = asWritableHandle(handle);
        try {
            FileStatus status = findStatusForSegment(handle.getSegmentName(), true);

            if (!isSealed(status.getPath())) {
                if (getEpoch(status) > this.epoch) {
                    throw new StorageNotPrimaryException(handle.getSegmentName());
                }
                makeReadOnly(status);
                Path sealedPath = getSealedFilePath(handle.getSegmentName());
                this.fileSystem.rename(status.getPath(), sealedPath);
            }
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e);
        }
        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "unseal", handle);
        try {
            FileStatus status = findStatusForSegment(handle.getSegmentName(), true);
            makeWrite(status);
            this.fileSystem.rename(status.getPath(), getFilePath(handle.getSegmentName(), this.epoch));
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e);
        }
        LoggerHelpers.traceLeave(log, "unseal", traceId, handle);
    }

    @Override
    public void concat(SegmentHandle target, long offset, String sourceSegment) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, offset, sourceSegment);

        target = asWritableHandle(target);
        // Check for target offset and whether it is sealed.
        FileStatus fileStatus = null;
        try {
            fileStatus = findStatusForSegment(target.getSegmentName(), true);

            if (isSealed(fileStatus.getPath())) {
                throw new StreamSegmentSealedException(target.getSegmentName());
            } else if (getEpoch(fileStatus) > this.epoch) {
                throw new StorageNotPrimaryException(target.getSegmentName());
            } else if (fileStatus.getLen() != offset) {
                throw new BadOffsetException(target.getSegmentName(), fileStatus.getLen(), offset);
            }
        } catch (IOException ex) {
            throw HDFSExceptionHelpers.convertException(target.getSegmentName(), ex);
        }

        try {
            FileStatus sourceFile = findStatusForSegment(sourceSegment, true);
            Preconditions.checkState(isSealed(sourceFile.getPath()),
                    "Cannot concat segment '%s' into '%s' because it is not sealed.", sourceSegment, target.getSegmentName());

            // Concat source file into target.
            this.fileSystem.concat(fileStatus.getPath(), new Path[]{sourceFile.getPath()});
        } catch (IOException ex) {
            throw HDFSExceptionHelpers.convertException(sourceSegment, ex);
        }
        LoggerHelpers.traceLeave(log, "concat", traceId, target, offset, sourceSegment);
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
        handle = asWritableHandle(handle);
        try {
            FileStatus statusForSegment = findStatusForSegment(handle.getSegmentName(), true);
            if (getEpoch(statusForSegment) > this.epoch && !isSealed(statusForSegment.getPath())) {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
            this.fileSystem.delete(statusForSegment.getPath(), true);
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e);
        }
        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        ensureInitializedAndNotClosed();
        return false;
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
        handle = asWritableHandle(handle);
        FileStatus status = null;
        try {
            status = findStatusForSegment(handle.getSegmentName(), true);
            if (isSealed(status.getPath())) {
                throw new StreamSegmentSealedException(handle.getSegmentName());
            }
            if (getEpochFromPath(status.getPath()) > this.epoch) {
                throw new StorageNotPrimaryException(handle.getSegmentName());
            }
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), e);
        }

        Timer timer = new Timer();
        try (FSDataOutputStream stream = this.fileSystem.append(status.getPath())) {
            if (offset != status.getLen()) {
                // Do the handle offset validation here, after we open the file. We want to throw FileNotFoundException
                // before we throw BadOffsetException.
                throw new BadOffsetException(handle.getSegmentName(), status.getLen(), offset);
            } else if (stream.getPos() != offset) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                log.warn("File changed detected for '{}'. Expected length = {}, actual length = {}.", status, status.getLen(), stream.getPos());
                throw new BadOffsetException(handle.getSegmentName(), status.getLen(), offset);
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
            throw HDFSExceptionHelpers.convertException(handle.getSegmentName(), ex);
        }

        HDFSMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        HDFSMetrics.WRITE_BYTES.add(length);
        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, length);
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        long fencedCount = 0;
        do {
            try {
                FileStatus fileStatus = findStatusForSegment(streamSegmentName, true);

                if (!isSealed(fileStatus.getPath())) {
                    if (getEpochFromPath(fileStatus.getPath()) > this.epoch) {
                        throw new StorageNotPrimaryException(streamSegmentName);
                    }

                    Path targetPath = getFilePath(streamSegmentName, this.epoch);
                    if (!targetPath.equals(fileStatus.getPath())) {
                        try {
                            this.fileSystem.rename(fileStatus.getPath(), targetPath);
                        } catch (FileNotFoundException e) {
                            //This happens when more than one host is trying to fence and only one of the host goes through.
                            //Retry the rename so that host with the highest epoch gets access.
                            //In the worst case, the current owner of the segment will win this race after a number of attempts
                            //  equal to the number of Segment Stores in the race. The high bound for this number of attempts
                            // is the total number of Segment Store instances in the cluster.
                            //It is safe to retry for MAX_EPOCH times as we are sure that the loop will never go that long.
                            log.warn("Race in fencing. More than two hosts trying to own the segment. Retrying");
                            fencedCount++;
                            continue;
                        }
                    }
                }
                //Ensure that file exists
                findStatusForSegment(streamSegmentName, true);
                return HDFSSegmentHandle.write(streamSegmentName);
            } catch (IOException e) {
                throw HDFSExceptionHelpers.convertException(streamSegmentName, e);
            }
            // Looping for the maximum possible number.
        } while (fencedCount <= this.epoch);
        LoggerHelpers.traceLeave(log, "openWrite", traceId, epoch);
        throw new StorageNotPrimaryException("Not able to fence out other writers.");
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        // Creates a file with the lowest possible epoch (0).
        // There is a possible race during create where more than one segmentstore may be trying to create a streamsegment.
        // If one create is delayed, it is possible that other segmentstore will be able to create the file with
        // epoch (0) and then rename it using its epoch (segment_<epoch>).
        //
        // To fix this, the create code checks whether a file with higher epoch exists.
        // If it does, it tries to remove the created file, and throws SegmentExistsException.

        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        // Create the segment using our own epoch.
        FileStatus[] status = null;
        try {
            status = findAllRaw(streamSegmentName);
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(streamSegmentName, e);
        }
        if (status != null && status.length > 0) {
            // Segment already exists; don't bother with anything else.
            throw HDFSExceptionHelpers.convertException(streamSegmentName, HDFSExceptionHelpers.segmentExistsException(streamSegmentName));
        }

        // Create the file for the segment with epoch 0.
        Path fullPath = getFilePath(streamSegmentName, 0);
        try {
            // Create the file, and then immediately close the returned OutputStream, so that HDFS may properly create the file.
            this.fileSystem.create(fullPath, READWRITE_PERMISSION, false, 0, this.config.getReplication(),
                    this.config.getBlockSize(), null).close();
            HDFSMetrics.CREATE_COUNT.inc();
            log.debug("Created '{}'.", fullPath);
        } catch (IOException e) {
            throw HDFSExceptionHelpers.convertException(streamSegmentName, e);
        }

        // If there is a race during creation, delete the file with epoch 0 and throw exception.
        // It is safe to delete the file as a file with higher epoch already exists. Any new operations will always
        // work the file with higher epoch than 0.
        try {
            status = findAllRaw(streamSegmentName);
            if (status != null && status.length > 1) {
                this.fileSystem.delete(fullPath, true);
                throw new StreamSegmentExistsException(streamSegmentName);
            }
        } catch (IOException e) {
            log.warn("Exception while deleting a file with epoch 0.", e);
        }
        LoggerHelpers.traceLeave(log, "create", traceId, streamSegmentName);

        // return handle
        return HDFSSegmentHandle.write(streamSegmentName);
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
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //Region HDFS helper methods.

    /**
     * Gets an array (not necessarily ordered) of FileStatus objects currently available for the given Segment.
     * These must be in the format specified by NAME_FORMAT (see EXAMPLE_NAME_FORMAT).
     */
    private FileStatus[] findAllRaw(String segmentName) throws IOException {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        String pattern = String.format(NAME_FORMAT, getPathPrefix(segmentName), SUFFIX_GLOB_REGEX);
        FileStatus[] files = this.fileSystem.globStatus(new Path(pattern));

        if (files.length > 1) {
            throw new IllegalArgumentException("More than one file");
        }
        return files;
    }

    /**
     * Gets an HDFS-friendly path prefix for the given Segment name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String segmentName) {
        return this.config.getHdfsRoot() + Path.SEPARATOR + segmentName;
    }

    /**
     * Gets the full HDFS Path to a file for the given Segment, startOffset and epoch.
     */
    private Path getFilePath(String segmentName, long epoch) {
        Preconditions.checkState(segmentName != null && segmentName.length() > 0, "segmentName must be non-null and non-empty");
        Preconditions.checkState(epoch >= 0, "epoch must be non-negative " + epoch);
        return new Path(String.format(NAME_FORMAT, getPathPrefix(segmentName), epoch));
    }

    /**
     * Gets the full HDFS path when sealed.
     */
    private Path getSealedFilePath(String segmentName) {
        Preconditions.checkState(segmentName != null && segmentName.length() > 0, "segmentName must be non-null and non-empty");
        return new Path(String.format(NAME_FORMAT, getPathPrefix(segmentName), SEALED));
    }

    /**
     * Gets the filestatus representing the segment.
     *
     * @param segmentName      The name of the Segment to retrieve for.
     * @param enforceExistence If true, it will throw a FileNotFoundException if no files are found, otherwise null is returned.
     * @return FileStatus of the HDFS file.
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
                .sorted(this::compareFileStatus)
                .collect(Collectors.toList());
        return result.get(result.size() - 1);
    }

    private int compareFileStatus(FileStatus f1, FileStatus f2) {
        try {
            return Long.compare(getEpoch(f1), getEpoch(f2));
        } catch (FileNameFormatException e) {
            throw new IllegalStateException(e);
        }
    }

    private long getEpoch(FileStatus status) throws FileNameFormatException {
        return getEpochFromPath(status.getPath());
    }

    private static String getSegmentNameFromPath(Path path) throws FileNameFormatException {
        String fileName = path.getName();
        int pos2 = fileName.lastIndexOf(PART_SEPARATOR);
        if (pos2 <= 0) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }
        return fileName.substring(0, pos2);
    }

    private static long getEpochFromPath(Path path) throws FileNameFormatException {
        String fileName = path.toString();
        int pos2 = fileName.lastIndexOf(PART_SEPARATOR);
        if (pos2 <= 0) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }
        if (pos2 == fileName.length() - 1 || fileName.regionMatches(pos2 + 1, SEALED, 0, SEALED.length())) {
            //File is sealed. This is the final version
            return MAX_EPOCH;
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
    private boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    /**
     * Makes the file represented by the given FileStatus read-only.
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

        this.fileSystem.setPermission(file.getPath(), READONLY_PERMISSION);
        log.debug("MakeReadOnly '{}'.", file.getPath());
        return true;
    }

    private boolean makeWrite(FileStatus file) throws IOException {
        this.fileSystem.setPermission(file.getPath(), READWRITE_PERMISSION);
        log.debug("MakeReadOnly '{}'.", file.getPath());
        return true;
    }

    private int readInternal(SegmentHandle handle, byte[] buffer, long offset, int bufferOffset, int length) throws IOException {
        //There is only one file per segment.
        FileStatus currentFile = findStatusForSegment(handle.getSegmentName(), true);
        try (FSDataInputStream stream = this.fileSystem.open(currentFile.getPath())) {
            stream.readFully(offset, buffer, bufferOffset, length);
        } catch (EOFException e) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the current size of segment.", offset));
        }
        return length;
    }

    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        try {
            return new HDFSSegmentIterator(this.fileSystem.listStatus(new Path(config.getHdfsRoot() + Path.SEPARATOR)),
                    fileStatus -> {
                        String fileName = fileStatus.getPath().getName();
                        int index = fileName.lastIndexOf(PART_SEPARATOR);
                        if (fileName.endsWith(PART_SEPARATOR + SEALED)) {
                            return true;
                        }
                        try {
                            Long.parseLong(fileName.substring(index + 1));
                        } catch (NumberFormatException nfe) {
                            return false;
                        }
                        return true;
                    });
        } catch (IOException e) {
            log.error("Exception occurred while listing the segments.", e);
            throw e;
        }
    }

    /**
     * Iterator for segments in HDFS Storage.
     */
    private static class HDFSSegmentIterator implements Iterator<SegmentProperties> {
        private final Iterator<SegmentProperties> results;

        HDFSSegmentIterator(FileStatus[] results, java.util.function.Predicate<FileStatus> patternMatchPredicate) {
            this.results = Arrays.asList(results).stream()
                    .filter(patternMatchPredicate)
                    .map(this::toSegmentProperties)
                    .iterator();
        }

        public SegmentProperties toSegmentProperties(FileStatus fileStatus) {
            try {
                boolean isSealed = isSealed(fileStatus.getPath());
                return StreamSegmentInformation.builder()
                        .name(getSegmentNameFromPath(fileStatus.getPath()))
                        .length(fileStatus.getLen())
                        .sealed(isSealed).build();
            } catch (FileNameFormatException e) {
                log.error("Exception occurred while transforming the object into SegmentProperties.");
                return null;
            }
        }

        /**
         * Method to check the presence of next element in the iterator.
         * @return true if the next element is there, else false.
         */
        @Override
        public boolean hasNext() {
            return results.hasNext();
        }

        /**
         * Method to return the next element in the iterator.
         * @return A newly created StreamSegmentInformation class.
         * @throws NoSuchElementException in case of an unexpected failure.
         */
        @Override
        public SegmentProperties next() throws NoSuchElementException {
            return results.next();
        }
    }
    //endregion
}