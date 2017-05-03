/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.service.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.service.storage.DataLogNotAvailableException;
import io.pravega.service.storage.DurableDataLog;
import io.pravega.service.storage.DurableDataLogException;
import io.pravega.service.storage.DurableDataLogFactory;
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
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient")
                                     .usingNamespace(zkClient.getNamespace() + this.config.getNamespace());
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
            if (!ExceptionHelpers.mustRethrow(ex)) {
                // Make sure we close anything we may have opened.
                close();
            }

            // ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to establish connection to ZooKeeper or BookKeeper.", ex);
        }
    }

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        Preconditions.checkState(this.bookKeeper.get() != null, "BookKeeperLogFactory is not initialized.");
        return new BookKeeperLog(containerId, this.zkClient, this.bookKeeper.get(), this.config, this.executor);
    }

    //endregion

    //region Initialization

    private BookKeeper startBookKeeperClient() throws Exception {
        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(this.config.getZkAddress())
                .setClientTcpNoDelay(true)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());
        config.setZkLedgersRootPath(this.config.getBkLedgerPath());
        return new BookKeeper(config);
    }

    //endregion
}
