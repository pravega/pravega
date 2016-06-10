package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.AsyncIterator;
import com.emc.logservice.common.Exceptions;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
class DistributedLogDataLog implements DurableDataLog {
    //region Members

    private final LogClient client;
    private final String logName;
    private LogHandle handle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogDataLog class.
     *
     * @param logName The name of the DistributedLog Log to use.
     * @param client  The DistributedLog Client to use.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If logName is an empty string.
     */
    public DistributedLogDataLog(String logName, LogClient client) {
        Exceptions.throwIfNull(client, "client");
        Exceptions.throwIfNullOfEmpty(logName, "logName");

        this.logName = logName;
        this.client = client;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.handle != null) {
            this.handle.close();
        }
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        Exceptions.throwIfIllegalState(this.handle == null, "DistributedLogDataLog is already initialized.");
        return this.client
                .getLogHandle(this.logName)
                .thenAccept(handle -> this.handle = handle);
    }

    @Override
    public CompletableFuture<Long> append(InputStream data, Duration timeout) {
        ensureInitialized();
        return this.handle.append(data, timeout);
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequence, Duration timeout) {
        ensureInitialized();
        return this.handle.truncate(upToSequence, timeout);
    }

    @Override
    public AsyncIterator<ReadItem> getReader(long afterSequence) throws DurableDataLogException {
        ensureInitialized();
        return this.handle.getReader(afterSequence);
    }

    @Override
    public int getMaxAppendLength() {
        ensureInitialized();
        return LogHandle.MaxAppendLength;
    }

    @Override
    public long getLastAppendSequence() {
        ensureInitialized();
        return this.handle.getLastTransactionId();
    }

    private void ensureInitialized() {
        Exceptions.throwIfIllegalState(this.handle != null, "DistributedLogDataLog is not initialized.");
    }

    //endregion
}
