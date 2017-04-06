/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.shared.MetricsNames;
import com.emc.pravega.common.Timer;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.shared.metrics.Counter;
import com.emc.pravega.shared.metrics.MetricsProvider;
import com.emc.pravega.shared.metrics.OpStatsLogger;
import com.emc.pravega.shared.metrics.StatsLogger;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;

/**
 * Storage adapter for a backing HDFS Store which implements fencing using a rename strategy..
 * Each segment is represented by a file with pattern <segment-name>_<owner_host_id>, where <owner_host_id> is optional
 * and means that the segment is owned by the Pravega host of the given id, i.e., it's fenced off.
 * <p>
 * Whenever a Segment Container ownership change happens (that is, when the Segment Container is moved to a different
 * Pravega Node), the HDFSStorage instance on the new node renames the file representing the segment to <segment-name>_<owner_host_id>.
 * This is done by the open call. The rename only happens if the segment is not sealed.
 * <p>
 * When a segment is sealed, it is renamed to its absolute name "segment-name" and marked as read-only.
 */
@Slf4j
class HDFSStorage implements Storage {
    //region Members

    private static final FsPermission SEALED_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
    private static final String LOG_ID = "HDFSStorage";
    private final Executor executor;
    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private FileSystem fileSystem;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config   The configuration to use.
     * @param executor The executor to use for running async operations.
     */
    HDFSStorage(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Initializes the HDFSStorage.
     *
     * @throws IOException If the initialization failed.
     */
    public void initialize() throws IOException {
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Exceptions.checkNotClosed(this.closed.get(), this);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.fileSystem = FileSystem.get(conf);
        log.info("{}: Initialized.", LOG_ID);
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
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> createSync(streamSegmentName), streamSegmentName, "create");
    }

    @Override
    public CompletableFuture<Void> open(String streamSegmentName) {
        return runAsync(() -> openWithFencing(streamSegmentName), streamSegmentName, "open");
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(() -> writeSync(streamSegmentName, offset, length, data), streamSegmentName, "write");
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> sealSync(streamSegmentName), streamSegmentName, "seal");
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        return runAsync(() -> concatSync(targetStreamSegmentName, offset, sourceStreamSegmentName), targetStreamSegmentName, "concat");
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return runAsync(() -> deleteSync(streamSegmentName), streamSegmentName, "delete");
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(() -> readSync(streamSegmentName, offset, buffer, bufferOffset, length), streamSegmentName, "read");
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> getStreamSegmentInfoSync(streamSegmentName), streamSegmentName, "getInfo");
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> existsSync(streamSegmentName), streamSegmentName, "exists");
    }

    //endregion

    //region Helpers

    private SegmentProperties createSync(String streamSegmentName) throws IOException, StreamSegmentExistsException {
        if (existsSync(streamSegmentName)) {
            throw new StreamSegmentExistsException(streamSegmentName);
        }
        String ownedFullPath = getOwnedSegmentFullPath(streamSegmentName);
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, ownedFullPath);
        this.fileSystem
                .create(new Path(ownedFullPath),
                        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                        false,
                        0,
                        this.config.getReplication(),
                        this.config.getBlockSize(),
                        null)
                .close();
        int numInstances = findAll(streamSegmentName).length;
        if (numInstances > 1) {
            log.warn("Segment '{}' has {} instances in Tier2 when the expected was only one.", streamSegmentName,
                    numInstances);
            this.fileSystem.delete(new Path(ownedFullPath), true);
            throw new StreamSegmentExistsException(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "create", traceId, streamSegmentName);
        return new StreamSegmentInformation(streamSegmentName,
                0,
                false,
                false,
                new ImmutableDate());
    }

    /**
     * Utility function to get the full name of the file representing the segment which is owned by the current
     * Pravega host.
     */
    private String getOwnedSegmentFullPath(String streamSegmentName) {
        return getCommonPartOfName(streamSegmentName) + "_" + this.config.getPravegaId();
    }

    /**
     * Gets the wildcard path string which represents all the files that could represent the current segment.
     */
    private FileStatus[] findAll(String streamSegmentName) throws IOException {
        String commonPart = getCommonPartOfName(streamSegmentName);
        String pattern = String.format("{%s,%s_[0-9]*}", commonPart, commonPart);
        return this.fileSystem.globStatus(new Path(pattern));
    }

    /**
     * Gets a FileStatus for a File representing the given StreamSegment if fencing is not desired.
     */
    private FileStatus findAny(String streamSegmentName) throws IOException {
        FileStatus[] statuses = findAll(streamSegmentName);
        if (statuses == null || statuses.length != 1) {
            String message = String.format("%s (Number of files is %s).", streamSegmentName, statuses == null ? 0 : statuses.length);
            if (statuses != null && statuses.length > 1) {
                log.warn("Segment not found: {}", message);
            }

            throw new FileNotFoundException(message);
        }

        return statuses[0];
    }

    /**
     * Gets a FileStatus for a File representing the given StreamSegment when fencing is desired.
     */
    private FileStatus findFenced(String streamSegmentName) throws IOException {
        FileStatus[] statuses = this.fileSystem.globStatus(new Path(getOwnedSegmentFullPath(streamSegmentName)));
        if (statuses == null || statuses.length != 1) {
            String message = String.format("%s (Number of files is %s).", streamSegmentName, statuses == null ? 0 : statuses.length);
            if (statuses != null && statuses.length > 1) {
                log.warn("Segment not found: {}", message);
            }

            throw new FileNotFoundException(message);
        }

        return statuses[0];
    }

    private String getCommonPartOfName(String streamSegmentName) {
        return this.config.getHdfsRoot() + "/" + streamSegmentName;
    }

    /**
     * "Fences" off the given segment by renaming it to something based off the segment's name and this HDFSStorage instance's id.
     */
    private void openWithFencing(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "fence", streamSegmentName);

        FileStatus status = findAny(streamSegmentName);
        if (isSealed(status)) {
            // Nothing to fence when sealed. Bail out.
            LoggerHelpers.traceLeave(log, "fence", traceId, streamSegmentName, streamSegmentName);
            return;
        }

        String ownedFullPath = getOwnedSegmentFullPath(streamSegmentName);
        if (!this.fileSystem.rename(status.getPath(), new Path(ownedFullPath))) {
            throw new IOException(String.format("Unable to fence Segment '%s'.", streamSegmentName));
        }

        LoggerHelpers.traceLeave(log, "fence", traceId, streamSegmentName, ownedFullPath);
    }

    /**
     * "Unfences" the given segment by renaming it to its original name. Currently the unfencing is done explicitly
     * only when the segment is sealed. Ideally we'd be unfencing a segment when we don't need it anymore, but that is
     * not necessary. If some other instance of HDFSStorage "fences out" this segment, then this instance will
     * automatically detect that and stop working on it.
     * <p>
     * As such, this method is only effective once the segment is sealed.
     *
     * @param streamSegmentName The name of the Segment to unfence.
     * @throws IOException                If some IO error occurred.
     * @throws StorageNotPrimaryException If this HDFSStorage instance is not the primary writer for this segment.
     */
    private void unfence(String streamSegmentName) throws IOException, StorageNotPrimaryException {
        long traceId = LoggerHelpers.traceEnter(log, "unfence", streamSegmentName);
        FileStatus status = findFenced(streamSegmentName);
        Preconditions.checkState(isSealed(status), "Cannot unfence segment '%s' because it is not sealed.", streamSegmentName);

        if (!this.fileSystem.rename(status.getPath(), new Path(getCommonPartOfName(streamSegmentName)))) {
            throw new IOException(String.format("Unable to unfence Segment '%s'.", streamSegmentName));
        }

        LoggerHelpers.traceLeave(log, "unfence", traceId, streamSegmentName, status.getPath().toString());
    }

    private void writeSync(String streamSegmentName, long offset, int length, InputStream data) throws BadOffsetException, IOException, StorageNotPrimaryException {
        long traceId = LoggerHelpers.traceEnter(log, "write", streamSegmentName, offset, length);
        Timer timer = new Timer();
        try (FSDataOutputStream stream = this.fileSystem.append(new Path(getOwnedSegmentFullPath(streamSegmentName)))) {
            if (stream.getPos() != offset) {
                throw new BadOffsetException(streamSegmentName, offset, stream.getPos());
            }

            IOUtils.copyBytes(data, stream, length);
            stream.flush();
        } catch (FileNotFoundException notFoundEx) {
            if (!handleFencingError(notFoundEx, streamSegmentName)) {
                throwSealedException(notFoundEx);
            }
        }

        Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        Metrics.WRITE_BYTES.add(length);
        LoggerHelpers.traceLeave(log, "write", traceId, streamSegmentName, offset, length);
    }

    private SegmentProperties sealSync(String streamSegmentName) throws IOException, StorageNotPrimaryException {
        String ownedFullPath = getOwnedSegmentFullPath(streamSegmentName);
        long traceId = LoggerHelpers.traceEnter(log, "seal", streamSegmentName, ownedFullPath);

        // Seal is reentrant. If a segment is already sealed, nothing is to be done. Because of this, we only require
        // the segment to be fenced off if it hadn't been sealed prior to this call. If already sealed, we are supposed
        // to do nothing.
        boolean mustUnfence = true;
        try {
            this.fileSystem.setPermission(new Path(ownedFullPath), SEALED_PERMISSION);
        } catch (FileNotFoundException notFoundEx) {
            if (!handleFencingError(notFoundEx, streamSegmentName)) {
                // Segment was already sealed, no need to unfence it again.
                mustUnfence = false;
            }
        }
        if (mustUnfence) {
            // Only try to unfence if the segment was not sealed to begin with (otherwise errors will crop up).
            unfence(streamSegmentName);
        }

        SegmentProperties result = getStreamSegmentInfoSync(streamSegmentName);
        LoggerHelpers.traceLeave(log, "seal", traceId, streamSegmentName);
        return result;
    }

    @VisibleForTesting
    protected SegmentProperties getStreamSegmentInfoSync(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        FileStatus status = findAny(streamSegmentName);

        SegmentProperties result = new StreamSegmentInformation(streamSegmentName,
                status.getLen(),
                status.getPermission().getUserAction() == FsAction.READ,
                false,
                new ImmutableDate(status.getModificationTime()));

        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName, result);
        return result;
    }

    private void concatSync(String targetStreamSegmentName, long offset, String sourceStreamSegmentName) throws IOException,
            BadOffsetException, StreamSegmentSealedException, StorageNotPrimaryException {
        long traceId = LoggerHelpers.traceEnter(log, "concat", targetStreamSegmentName, offset, sourceStreamSegmentName);
        FileStatus targetStatus = null;
        try {
            targetStatus = findFenced(targetStreamSegmentName);
            if (targetStatus.getLen() != offset) {
                throw new BadOffsetException(targetStreamSegmentName, offset, targetStatus.getLen());
            }
        } catch (FileNotFoundException notFoundEx) {
            if (!handleFencingError(notFoundEx, targetStreamSegmentName)) {
                throwSealedException(notFoundEx);
            }
        }

        assert targetStatus != null : "targetStatus is null, but it shouldn't be";
        FileStatus sourceStatus = findAny(sourceStreamSegmentName);
        Preconditions.checkState(isSealed(sourceStatus),
                "Cannot concat segment '%s' into '%s' because it is not sealed.", sourceStreamSegmentName, targetStreamSegmentName);

        this.fileSystem.concat(targetStatus.getPath(),
                new Path[]{ sourceStatus.getPath() });
        LoggerHelpers.traceLeave(log, "concat", traceId, targetStreamSegmentName, offset, sourceStreamSegmentName);
    }

    private void deleteSync(String streamSegmentName) throws IOException, StorageNotPrimaryException {
        String ownedFullPath = getOwnedSegmentFullPath(streamSegmentName);
        long traceId = LoggerHelpers.traceEnter(log, "delete", streamSegmentName, ownedFullPath);
        try {
            tryDelete(ownedFullPath, streamSegmentName);
        } catch (FileNotFoundException notFoundEx) {
            if (!handleFencingError(notFoundEx, streamSegmentName)) {
                tryDelete(getCommonPartOfName(streamSegmentName), streamSegmentName);
            }
        }

        LoggerHelpers.traceLeave(log, "delete", traceId, streamSegmentName);
    }

    private void tryDelete(String path, String streamSegmentName) throws IOException {
        if (!this.fileSystem.delete(new Path(path), false)) {
            throw new FileNotFoundException(streamSegmentName);
        }
    }

    /**
     * Finds the file containing the given offset for the given segment.
     * Reads from that file.
     */
    private int readSync(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        long traceId = LoggerHelpers.traceEnter(log, "read", streamSegmentName, offset, length);
        Timer timer = new Timer();
        FileStatus fs = findAny(streamSegmentName);
        FSDataInputStream stream = fileSystem.open(fs.getPath());
        int retVal = stream.read(offset, buffer, bufferOffset, length);
        if (retVal < 0) {
            // -1 is usually a code for invalid args; check to see if we were supplied with an offset that exceeds the length of the segment.
            long segmentLength = getStreamSegmentInfoSync(streamSegmentName).getLength();
            if (offset >= segmentLength) {
                if (segmentLength == 0) {
                    // Empty segment.
                    return 0;
                }

                throw new IllegalArgumentException(String.format("Read offset (%s) is beyond the length of the segment (%s).", offset, segmentLength));
            }
        }

        Metrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
        Metrics.READ_BYTES.add(length);
        LoggerHelpers.traceLeave(log, "read", traceId, streamSegmentName, offset, retVal);
        return retVal;
    }

    private boolean existsSync(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
        boolean result = findAll(streamSegmentName).length > 0;
        LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName, result);
        return result;
    }

    private CompletableFuture<Void> runAsync(RunnableWithException syncCode, String streamSegmentName, String action) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            try {
                syncCode.run();
            } catch (Exception e) {
                throw new CompletionException(HDFSExceptionHelpers.translateFromException(streamSegmentName, e));
            }
        }, this.executor);
    }

    private <T> CompletableFuture<T> supplyAsync(Callable<T> syncCode, String streamSegmentName, String action) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.supplyAsync(() -> {
            T result;
            try {
                result = syncCode.call();
            } catch (Exception e) {
                throw new CompletionException(HDFSExceptionHelpers.translateFromException(streamSegmentName, e));
            }

            return result;
        }, this.executor);
    }

    private boolean isSealed(FileStatus status) {
        return status.getPermission().getUserAction() == FsAction.READ;
    }

    /**
     * Handles a FileNotFoundException in the context of a modify operation (write, seal, concat, delete).
     * Converts the FileNotFoundException into a StorageNotPrimaryException and throws that, if a fence-out was detected.
     *
     * @param notFoundEx        The original exception that got caught.
     * @param streamSegmentName The name of the Segment that is the target for the executed IOCall.
     * @return True if the there exists a file with this Segment's name that is Sealed (in other words, if the segment
     * was not fenced off but sealed). This method never returns False - in that case an Exception will be thrown instead.
     */
    private boolean handleFencingError(FileNotFoundException notFoundEx, String streamSegmentName) throws IOException, StorageNotPrimaryException {
        // Attempt to figure out the current state of the Segment in HDFS.
        FileStatus[] all = null;
        try {
            all = findAll(streamSegmentName);
        } catch (IOException findAllEx) {
            // We couldn't do it. Suppress this exception (if we can), and rethrow the original one.
            if (ExceptionHelpers.mustRethrow(findAllEx)) {
                throw findAllEx;
            }

            notFoundEx.addSuppressed(findAllEx);
        }

        if (all == null || all.length == 0) {
            throw notFoundEx;
        }

        if (isSealed(all[0])) {
            // We ran into a FileNotFoundException, then found a file that is sealed; the behavior here is dependent
            // on the method invoking this, hence the need for a configurable callback
            return false;
        } else {
            // The segment still exists in HDFS, but under a different name. As such, we have been fenced out.
            val toThrow = new StorageNotPrimaryException(streamSegmentName);
            toThrow.addSuppressed(notFoundEx);
            throw toThrow;
        }
    }

    private void throwSealedException(Throwable cause) throws IOException {
        val toThrow = new AclException("Segment is sealed.");
        toThrow.addSuppressed(cause);
        throw toThrow;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //region Metrics

    private static class Metrics {
        private static final StatsLogger HDFS_LOGGER = MetricsProvider.createStatsLogger("hdfs");
        static final OpStatsLogger READ_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_READ_LATENCY);
        static final OpStatsLogger WRITE_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_WRITE_LATENCY);
        static final Counter READ_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_READ_BYTES);
        static final Counter WRITE_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_WRITE_BYTES);
    }

    //endregion
}
