/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
class DistributedLogDataLog implements DurableDataLog {
    //region Members

    private final LogClient client;
    private final String logName;
    private final AtomicReference<DLSNAddress> truncatedAddress;
    private final Executor executor;
    private LogHandle handle;
    private LogHandle truncateHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogDataLog class.
     *
     * @param logName  The name of the DistributedLog Log to use.
     * @param client   The DistributedLog Client to use.
     * @param executor An Executor to use for async operations.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If logName is an empty string.
     */
    DistributedLogDataLog(String logName, LogClient client, Executor executor) {
        Preconditions.checkNotNull(client, "client");
        Exceptions.checkNotNullOrEmpty(logName, "logName");
        Preconditions.checkNotNull(executor, "executor");

        this.logName = logName;
        this.client = client;
        this.executor = executor;
        this.truncatedAddress = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.handle != null) {
            this.handle.close();
            this.handle = null;
        }

        if (this.truncateHandle != null) {
            this.truncateHandle.close();
            this.truncateHandle = null;
        }
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        Preconditions.checkState(this.handle == null, "DistributedLogDataLog is already initialized.");
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
                close();
            }

            throw ex;
        }
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        ensureInitialized();
        return this.handle.append(data, timeout);
    }

    @Override
    public CompletableFuture<Boolean> truncate(LogAddress logAddress, Duration timeout) {
        ensureInitialized();
        Preconditions.checkArgument(logAddress instanceof DLSNAddress, "Invalid logAddress. Expected a DLSNAddress.");
        DLSNAddress dlsnAddress = (DLSNAddress) logAddress;
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.handle
                .truncate(dlsnAddress, timer.getRemaining())
                .thenComposeAsync(v -> this.truncateHandle.append(new ByteArrayInputStream(dlsnAddress.serialize()), timer.getRemaining()), this.executor)
                .thenComposeAsync(truncateAddress -> this.truncateHandle.truncate((DLSNAddress) truncateAddress, timer.getRemaining()), this.executor)
                .thenApply(v -> {
                    this.truncatedAddress.set(dlsnAddress);
                    return true;
                });
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ensureInitialized();
        DLSNAddress truncatedAddress = this.truncatedAddress.get();
        if (truncatedAddress != null) {
            afterSequence = Math.max(afterSequence, truncatedAddress.getSequence());
        }

        return this.handle.getReader(afterSequence);
    }

    @Override
    public int getMaxAppendLength() {
        ensureInitialized();
        return LogHandle.MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        ensureInitialized();
        return this.handle.getLastTransactionId();
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.handle != null, "DistributedLogDataLog is not initialized.");
    }

    //endregion
}
