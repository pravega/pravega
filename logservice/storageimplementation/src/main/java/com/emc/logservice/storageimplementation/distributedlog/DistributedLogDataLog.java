package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.storageabstraction.DurableDataLog;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
public class DistributedLogDataLog implements DurableDataLog {
    @Override
    public CompletableFuture<Long> append(byte[] data, Duration timeout) {
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
        return 0;
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
