/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;

/**
 * Wrapper for a BookKeeperLog which only exposes methods that should be used for debugging/admin tools.
 * NOTE: this class is not meant to be used for regular, production code. It exposes operations that should only be executed
 * from the admin tools.
 */
public class DebugLogWrapper implements AutoCloseable {
    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final BookKeeperLog log;
    private final BookKeeper bkClient;
    private final BookKeeperConfig config;
    private final AtomicBoolean initialized;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DebugLogWrapper class.
     *
     * @param logId      The Id of the BookKeeperLog to wrap.
     * @param zkClient   A pointer to the CuratorFramework client to use.
     * @param bookKeeper A pointer to the BookKeeper client to use.
     * @param config     BookKeeperConfig to use.
     * @param executor   An Executor to use for async operations.
     */
    DebugLogWrapper(int logId, CuratorFramework zkClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executor) {
        this.log = new BookKeeperLog(logId, zkClient, bookKeeper, config, executor);
        this.bkClient = bookKeeper;
        this.config = config;
        this.initialized = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.log.close();
    }

    //endregion

    //region Operations

    /**
     * Creates a special DurableDataLog wrapping the BookKeeperLog that does only supports reading from the log. It does
     * not support initialization or otherwise modifications to the log. Accessing this log will not interfere with other
     * active writes to this log (i.e., it will not fence anyone out or close Ledgers that shouldn't be closed).
     *
     * @return A new DurableDataLog instance.
     * @throws DataLogInitializationException If an exception occurred fetching metadata from ZooKeeper.
     */
    public DurableDataLog asReadOnly() throws DataLogInitializationException {
        return new ReadOnlyBooKeeperLog(this.log.loadMetadata());
    }

    /**
     * Loads a fresh copy BookKeeperLog Metadata from ZooKeeper, without doing any sort of fencing or otherwise modifying
     * it.
     *
     * @return A new instance of the LogMetadata class, or null if no such metadata exists (most likely due to this being
     * the first time accessing this log).
     * @throws DataLogInitializationException If an Exception occurred.
     */
    public ReadOnlyLogMetadata fetchMetadata() throws DataLogInitializationException {
        return this.log.loadMetadata();
    }

    /**
     * Opens a ledger for reading purposes (does not fence it).
     *
     * @param ledgerMetadata LedgerMetadata for the ledger to open.
     * @return A BookKeeper LedgerHandle representing the ledger.
     * @throws DurableDataLogException If an exception occurred.
     */
    public LedgerHandle openLedgerNoFencing(LedgerMetadata ledgerMetadata) throws DurableDataLogException {
        return Ledgers.openRead(ledgerMetadata.getLedgerId(), this.bkClient, this.config);
    }

    /**
     * Updates the Metadata for this BookKeeperLog in ZooKeeper by setting its Enabled flag to true.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void enable() throws DurableDataLogException {
        this.log.enable();
    }

    /**
     * Open-Fences the BookKeeperLog (initializes it), then updates the Metadata for it in ZooKeeper by setting its
     * Enabled flag to false.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void disable() throws DurableDataLogException {
        initialize();
        this.log.disable();
    }

    private void initialize() throws DurableDataLogException {
        if (this.initialized.compareAndSet(false, true)) {
            try {
                this.log.initialize(DEFAULT_TIMEOUT);
            } catch (Exception ex) {
                this.initialized.set(false);
                throw ex;
            }
        }
    }

    //endregion

    //region ReadOnlyBookKeeperLog

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class ReadOnlyBooKeeperLog implements DurableDataLog {
        private final LogMetadata logMetadata;

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
            return new LogReader(this.logMetadata, DebugLogWrapper.this.bkClient, DebugLogWrapper.this.config);
        }

        @Override
        public int getMaxAppendLength() {
            return BookKeeperConfig.MAX_APPEND_LENGTH;
        }

        @Override
        public long getEpoch() {
            return this.logMetadata.getEpoch();
        }

        @Override
        public QueueStats getQueueStatistics() {
            return null;
        }

        @Override
        public void initialize(Duration timeout) throws DurableDataLogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enable() throws DurableDataLogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disable() throws DurableDataLogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<LogAddress> append(ArrayView data, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}
