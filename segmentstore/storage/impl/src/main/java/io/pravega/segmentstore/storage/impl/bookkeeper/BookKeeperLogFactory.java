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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for BookKeeperLogs.
 */
@Slf4j
public class BookKeeperLogFactory implements DurableDataLogFactory {
    //region Members

    private final String namespace;
    private final CuratorFramework zkClient;
    private final AtomicReference<BookKeeper> bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeperLogFactory class.
     *
     * @param config   The configuration to use for all instances created.
     * @param zkClient ZooKeeper Client to use.
     * @param executor An executor to use for async operations.
     */
    public BookKeeperLogFactory(BookKeeperConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.namespace = zkClient.getNamespace();
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient")
                                     .usingNamespace(this.namespace + this.config.getZkMetadataPath());
        this.bookKeeper = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        val bk = this.bookKeeper.getAndSet(null);
        if (bk != null) {
            try {
                bk.close();
            } catch (Exception ex) {
                log.error("Unable to close BookKeeper client.", ex);
            }
        }
    }

    //endregion

    //region DurableDataLogFactory Implementation

    @Override
    public void initialize() throws DurableDataLogException {
        Preconditions.checkState(this.bookKeeper.get() == null, "BookKeeperLogFactory is already initialized.");
        try {
            this.bookKeeper.set(startBookKeeperClient());
        } catch (IllegalArgumentException | NullPointerException ex) {
            // Most likely a configuration issue; re-throw as is.
            close();
            throw ex;
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // Make sure we close anything we may have opened.
                close();
            }

            // ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to establish connection to ZooKeeper or BookKeeper.", ex);
        }
    }

    @Override
    public DurableDataLog createDurableDataLog(int logId) {
        Preconditions.checkState(this.bookKeeper.get() != null, "BookKeeperLogFactory is not initialized.");
        return new BookKeeperLog(logId, this.zkClient, this.bookKeeper.get(), this.config, this.executor);
    }

    /**
     * Creates a new DebugLogWrapper that can be used for debugging purposes. This should not be used for regular operations.
     *
     * @param logId Id of the Log to create a wrapper for.
     * @return A new instance of the DebugLogWrapper class.
     */
    public DebugLogWrapper createDebugLogWrapper(int logId) {
        Preconditions.checkState(this.bookKeeper.get() != null, "BookKeeperLogFactory is not initialized.");
        return new DebugLogWrapper(logId, this.zkClient, this.bookKeeper.get(), this.config, this.executor);
    }

    /**
     * Gets a pointer to the BookKeeper client used by this BookKeeperLogFactory. This should only be used for testing or
     * admin tool purposes only. It should not be used for regular operations.
     *
     * @return The BookKeeper client.
     */
    @VisibleForTesting
    public BookKeeper getBookKeeperClient() {
        return this.bookKeeper.get();
    }

    //endregion

    //region Initialization

    private BookKeeper startBookKeeperClient() throws Exception {
        // These two are in Seconds, not Millis.
        int writeTimeout = (int) Math.ceil(this.config.getBkWriteTimeoutMillis() / 1000.0);
        int readTimeout = (int) Math.ceil(this.config.getBkReadTimeoutMillis() / 1000.0);
        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(this.config.getZkAddress())
                .setClientTcpNoDelay(true)
                .setAddEntryTimeout(writeTimeout)
                .setReadEntryTimeout(readTimeout)
                .setGetBookieInfoTimeout(readTimeout)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());

        if (this.config.isTLSEnabled()) {
            config = (ClientConfiguration) config.setTLSProvider("OpenSSL");
            config = config.setTLSTrustStore(this.config.getTlsTrustStore());
            config.setTLSTrustStorePasswordPath(this.config.getTlsTrustStorePasswordPath());
        }

        if (this.config.getBkLedgerPath().isEmpty()) {
            config.setZkLedgersRootPath("/" + this.namespace + "/bookkeeper/ledgers");
        } else {
            config.setZkLedgersRootPath(this.config.getBkLedgerPath());
        }
        return new BookKeeper(config);
    }

    //endregion
}
