/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.storage.DataLogNotAvailableException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.service.storage.WriteFailureException;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
@ThreadSafe
@Slf4j
class DistributedLogDataLog implements DurableDataLog {
    //region Members

    private final LogClient client;
    private final String logName;
    private final AtomicReference<DLSNAddress> truncatedAddress;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed;
    private final Retry.RetryAndThrowBase<Exception> retryPolicy;
    private final Object handleLock = new Object();
    private final String traceObjectId;
    @GuardedBy("handleLock")
    private LogHandle handle;
    @GuardedBy("handleLock")
    private LogHandle truncateHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogDataLog class.
     *
     * @param logName  The name of the DistributedLog Log to use.
     * @param config   The DistributedLog Configuration to use.
     * @param client   The DistributedLog Client to use.
     * @param executor An Executor to use for async operations.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If logName is an empty string.
     */
    DistributedLogDataLog(String logName, DistributedLogConfig config, LogClient client, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(client, "client");
        Exceptions.checkNotNullOrEmpty(logName, "logName");
        Preconditions.checkNotNull(executor, "executor");

        this.logName = logName;
        this.client = client;
        this.executor = executor;
        this.truncatedAddress = new AtomicReference<>();
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("Log[%s]", logName);
        this.retryPolicy = config.getRetryPolicy()
                                 .retryWhen(DistributedLogDataLog::isRetryable)
                                 .throwingOn(Exception.class);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            closeHandles();
        }
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        openHandles();
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        ensureActive();
        return withRetries(() -> getHandle().append(data), () -> resetInput(data));
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress logAddress, Duration timeout) {
        ensureActive();
        Preconditions.checkArgument(logAddress instanceof DLSNAddress, "Invalid logAddress. Expected a DLSNAddress.");
        DLSNAddress dlsnAddress = (DLSNAddress) logAddress;
        return withRetries(() -> getHandle().truncate(dlsnAddress))
                .thenComposeAsync(
                        v -> withRetries(() -> getTruncateHandle().append(new ByteArrayInputStream(dlsnAddress.serialize()))),
                        this.executor)
                .thenComposeAsync(
                        truncateAddress -> withRetries(() -> getTruncateHandle().truncate((DLSNAddress) truncateAddress)),
                        this.executor)
                .thenRun(() -> this.truncatedAddress.set(dlsnAddress));
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ensureActive();
        DLSNAddress truncatedAddress = this.truncatedAddress.get();
        if (truncatedAddress != null) {
            afterSequence = Math.max(afterSequence, truncatedAddress.getSequence());
        }

        return getHandle().getReader(afterSequence);
    }

    @Override
    public int getMaxAppendLength() {
        ensureActive();
        return LogHandle.MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        ensureActive();
        return getHandle().getLastTransactionId();
    }

    private void ensureActive() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.handleLock) {
            Preconditions.checkState(this.handle != null, "DistributedLogDataLog is not initialized.");
        }
    }

    //endregion

    //region Helpers

    /**
     * Gets a pointer to the LogHandle for the primary log. If the handles are not yet initialized, they will both be
     * reopened.
     */
    @SneakyThrows(DurableDataLogException.class)
    private LogHandle getHandle() {
        synchronized (this.handleLock) {
            if (!areHandlesOpen()) {
                openHandles();
            }

            return this.handle;
        }
    }

    /**
     * Gets a pointer to the LogHandle for the truncation log. If the handles are not yet initialized, they will both be
     * reopened.
     */
    @SneakyThrows(DurableDataLogException.class)
    private LogHandle getTruncateHandle() {
        synchronized (this.handleLock) {
            if (!areHandlesOpen()) {
                openHandles();
            }

            return this.truncateHandle;
        }
    }

    /**
     * Attempts to open the handles.
     *
     * @throws DurableDataLogException If either handle could not be opened.
     */
    @GuardedBy("handleLock")
    private void openHandles() throws DurableDataLogException {
        Preconditions.checkState(!areHandlesOpen(), "DistributedLogDataLog is already initialized.");
        try {
            this.handle = this.client.getLogHandle(this.logName);
            this.truncateHandle = this.client.getLogHandle(this.logName + "#truncation");

            // Figure out the exact location of the last truncation, if any.
            long lastTruncateSeq = this.truncateHandle.getLastTransactionId();
            if (lastTruncateSeq > LogHandle.START_TRANSACTION_ID) {
                try (val reader = this.truncateHandle.getReader(lastTruncateSeq - 1)) {
                    val lastTruncateItem = reader.getNext();
                    this.truncatedAddress.set(DLSNAddress.deserialize(lastTruncateItem.getPayload()));
                }
            }
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                // Make sure we closed whatever resources or locks we acquired.
                log.warn("{}: Could not open handles; closing.", this.traceObjectId, ex);
                close();
            }

            throw ex;
        }
    }

    /**
     * Determines whether the Log Handles are open.
     */
    @GuardedBy("handleLock")
    private boolean areHandlesOpen() {
        return this.handle != null && this.truncateHandle != null;
    }

    /**
     * Closes any open Log Handle.
     */
    private void closeHandles() {
        synchronized (this.handleLock) {
            if (this.handle != null) {
                this.handle.close();
                this.handle = null;
            }

            if (this.truncateHandle != null) {
                this.truncateHandle.close();
                this.truncateHandle = null;
            }
        }

        log.info("{}: Closed handles.", this.traceObjectId);
    }

    /**
     * Executes the given futureSupplier with this class' Retry Policy and default Exception Handler.
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier) {
        return this.retryPolicy.runAsync(() -> futureSupplier.get().exceptionally(this::handleException), this.executor);
    }

    /**
     * Executes the given futureSupplier with this class' Retry Policy and default Exception Handler.
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Runnable onRetry) {
        AtomicInteger retryCount = new AtomicInteger(0);
        return this.retryPolicy.runAsync(() -> {
            if (onRetry != null && retryCount.incrementAndGet() > 1) {
                onRetry.run();
            }

            return futureSupplier.get().exceptionally(this::handleException);
        }, this.executor);
    }

    /**
     * Handles an exception from the code executed with a Retry Policy. If the exception is retryable, the Log Handles
     * are closed (in hopes that reopening them would solve the problem.
     */
    @SneakyThrows(Throwable.class)
    private <T> T handleException(Throwable ex) {
        if (isRetryable(ex) || (ex instanceof ObjectClosedException && !this.closed.get())) {
            // Close the handles upon an exception. They will be reopened when the operation is retried.
            log.warn("{}: Caught retryable exception.", this.traceObjectId, ex);
            closeHandles();
        }

        // Rethrow the original exception.
        throw ex;
    }

    /**
     * Determines whether the given exception can be retried.
     */
    static boolean isRetryable(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        return ex instanceof DataLogNotAvailableException
                || ex instanceof WriteFailureException;
    }

    @SneakyThrows(IOException.class)
    private void resetInput(InputStream data) {
        if (data.markSupported()) {
            data.reset();
        }
    }

    //endregion
}
