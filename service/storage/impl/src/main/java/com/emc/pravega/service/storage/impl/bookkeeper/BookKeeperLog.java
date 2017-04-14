/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;

/**
 * Apache BookKeeper implementation of the DurableDataLog interface.
 */
class BookKeeperLog implements DurableDataLog {
    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 100;
    private final CuratorFramework curatorClient;
    private final BookKeeper bookKeeper;
    private final BookKeeperConfig config;
    private final AtomicReference<LedgerHandle> writeLedger;
    private final ScheduledExecutorService executorService;

    //region Constructor

    BookKeeperLog(CuratorFramework curatorClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(curatorClient, "curatorClient");
        Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executorService, "executorService");

        this.curatorClient = curatorClient;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.executorService = executorService;
        this.writeLedger =new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {

    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        // TODO: Open the node, get metadata.
        // Fence out ledgers.
        // Create new ledger.
        // Update node with new ledger.
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        // TODO: use the async write API.
        return null;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        // TODO: see PDP.
        return null;
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        // TODO: use the async reader API
        return null;
    }

    @Override
    public int getMaxAppendLength() {
        return MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        return 0;
    }

    @Override
    public long getEpoch() {
        return 0;
    }

    //endregion

    private String getLogNodePath(String zkNamespace, String logId) {
        return String.format("%s/%s", zkNamespace, logId);
    }
}
