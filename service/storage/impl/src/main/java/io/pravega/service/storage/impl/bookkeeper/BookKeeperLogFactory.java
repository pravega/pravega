/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.DataLogNotAvailableException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.google.common.base.Preconditions;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Factory for BookKeeperLogs.
 */
@Slf4j
public class BookKeeperLogFactory implements DurableDataLogFactory {
    //region Members

    private static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);
    private final AtomicReference<CuratorFramework> curator;
    private final AtomicReference<BookKeeper> bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeperLogFactory class.
     *
     * @param config   The configuration to use for all instances created.
     * @param executor An executor to use for async operations.
     */
    public BookKeeperLogFactory(BookKeeperConfig config, ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.curator = new AtomicReference<>();
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

        val curator = this.curator.getAndSet(null);
        if (curator != null) {
            curator.close();
        }
    }

    //endregion

    //region Initialization

    /**
     * Initializes the BookKeeperLogFactory by attempting to connect to ZooKeeper and BookKeeper.
     *
     * @throws DurableDataLogException If an exception occurred. The causing exception is usually wrapped in this one.
     */
    public void initialize() throws DurableDataLogException {
        Preconditions.checkState(this.curator.get() == null, "BookKeeperLogFactory is already initialized.");
        assert this.bookKeeper.get() == null : "curator == null but bookKeeper != null";
        try {
            this.curator.set(startCuratorClient());
            this.bookKeeper.set(startBookKeeperClient());
        } catch (IllegalArgumentException | NullPointerException ex) {
            // Most likely a configuration issue; re-throw as is.
            close();
            throw ex;
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                // Make sure we close anything we may have opened.
                close();
            }

            // ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to establish connection to ZooKeeper or BookKeeper.", ex);
        }
    }

    private CuratorFramework startCuratorClient() {
        val curator = CuratorFrameworkFactory.newClient(
                this.config.getZkAddress(),
                (int) this.config.getZkSessionTimeout().toMillis(),
                (int) this.config.getZkConnectionTimeout().toMillis(),
                CURATOR_RETRY_POLICY);

        try {
            curator.start();
            Exceptions.handleInterrupted(() -> {
                curator.blockUntilConnected((int) this.config.getZkConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
            });
            return curator;
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                curator.close();
            }

            throw ex;
        }
    }

    private BookKeeper startBookKeeperClient() throws Exception {
        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(this.config.getZkAddress())
                .setClientTcpNoDelay(true)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());
        return new BookKeeper(config);
    }

    //endregion

    //region DurableDataLogFactory Implementation

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        Preconditions.checkState(this.curator.get() != null, "BookKeeperLogFactory is not initialized.");
        assert this.bookKeeper.get() != null : "curator != null but bookKeeper == null";
        return new BookKeeperLog(containerId, this.curator.get(), this.bookKeeper.get(), this.config, this.executor);
    }

    //endregion
}
