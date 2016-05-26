package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.storageabstraction.DurableDataLog;

import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
public class DistributedLogDataLog implements DurableDataLog {
    /**
     * Maximum append length, as specified by DistributedLog (this is hardcoded inside DLog's code).
     */
    private static final int MaxAppendLength = 1024*1024 - 8 *1024;

    @Override
    public CompletableFuture<Long> append(InputStream data, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequence, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Iterator<ReadItem>> read(long afterSequence, int maxCount, Duration timeout) {
        return null;
    }

    @Override
    public int getMaxAppendLength() {
        return MaxAppendLength;
    }

    @Override
    public long getLastAppendSequence() {
        return 0;
    }

    @Override
    public CompletableFuture<Void> recover(Duration timeout) {
        return null;
    }
}
