/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.MetricsNames;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.common.metrics.Counter;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.OpStatsLogger;
import com.emc.pravega.common.metrics.StatsLogger;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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

    private final Executor executor;
    private final HDFSStorageConfig config;
    private final AtomicBoolean closed;
    private FileSystemOperation.OperationContext context;

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
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", this.config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        this.context = new FileSystemOperation.OperationContext(epoch, FileSystem.get(conf), this.config);
        log.info("Initialized (HDFSHost = '{}', Epoch = {}).", this.config.getHdfsHostURL(), epoch);
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(new CreateOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return supplyAsync(new OpenWriteOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(new OpenReadOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(new WriteOperation(asWritableHandle(handle), offset, data, length, this.context));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return runAsync(new SealOperation(asWritableHandle(handle), this.context));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, SegmentHandle sourceHandle, Duration timeout) {
        return runAsync(new ConcatOperation(asWritableHandle(targetHandle), offset, asReadableHandle(sourceHandle), this.context));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return runAsync(new DeleteOperation(asWritableHandle(handle), this.context));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(new ReadOperation(asReadableHandle(handle), offset, buffer, bufferOffset, length, this.context));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(new GetInfoOperation(streamSegmentName, this.context));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(new ExistsOperation(streamSegmentName, this.context));
    }

    //endregion

    //region Helpers

    /**
     * Executes the given FileSystem asynchronously and returns a Future that will be completed when it finishes.
     */
    private <T extends FileSystemOperation & RunnableWithException> CompletableFuture<Void> runAsync(T operation) {
        ensureInitializedAndNotClosed();
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                operation.run();
                result.complete(null);
            } catch (Throwable e) {
                handleException(e, operation, result);
            }
        });

        return result;
    }

    /**
     * Executes the given FileSystemOperation asynchronously and returns a Future that will be completed with the result.
     */
    private <R, T extends FileSystemOperation & Callable<R>> CompletableFuture<R> supplyAsync(T operation) {
        ensureInitializedAndNotClosed();
        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                handleException(e, operation, result);
            }
        });

        return result;
    }

    private void handleException(Throwable e, FileSystemOperation<?> operation, CompletableFuture<?> result) {
        String segmentName = operation.getTarget() instanceof SegmentHandle
                ? ((SegmentHandle) operation.getTarget()).getSegmentName()
                : operation.getTarget().toString();
        result.completeExceptionally(HDFSExceptionHelpers.translateFromException(segmentName, e));
    }

    private HDFSSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    private HDFSSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof HDFSSegmentHandle, "handle must be of type HDFSSegmentHandle.");
        return (HDFSSegmentHandle) handle;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.context != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //TODO: reintegrate metrics.
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
