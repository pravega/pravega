/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.VisibleForDebugging;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DurableDataLogException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;

/**
 * Wrapper for a BookKeeperLog which only exposes methods that should be used for debugging/admin tools.
 */
@VisibleForDebugging
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
     */
    public void enable() throws DurableDataLogException {
        this.log.enable();
    }

    /**
     * Open-Fences the BookKeeperLog (initializes it), then updates the Metadata for it in ZooKeeper by setting its
     * Enabled flag to false.
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
                ex.printStackTrace();
                this.initialized.set(false);
                throw ex;
            }
        }
    }

    //endregion
}
