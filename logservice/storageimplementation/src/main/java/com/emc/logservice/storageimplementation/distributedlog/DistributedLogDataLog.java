package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.AsyncIterator;
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

    /**
     * Maximum append length, as specified by DistributedLog (this is hardcoded inside DLog's code).
     */
    private static final int MaxAppendLength = 1024 * 1024 - 8 * 1024;

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
        if(client == null){
            throw new NullPointerException("client");
        }

        if(logName == null){
            throw new NullPointerException("logName");
        }

        if(logName.length() == 0){
            throw new IllegalArgumentException("logName");
        }

        this.logName = logName;
        this.client = client;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws DurableDataLogException {
        if (this.handle != null) {
            this.handle.close();
        }
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        if (this.handle != null) {
            throw new IllegalStateException("DistributedLogDataLog is already initialized.");
        }

        return this.client.getLogHandle(this.logName).thenAccept(handle -> this.handle = handle);
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
        return this.MaxAppendLength;
    }

    @Override
    public long getLastAppendSequence() {
        ensureInitialized();
        return this.handle.getLastTransactionId();
    }

    private void ensureInitialized() {
        if (this.handle == null) {
            throw new IllegalStateException("DistributedLogDataLog is not initialized.");
        }
    }

    //endregion
}
