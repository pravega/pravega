/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.CommonConfigurationKeys;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.concurrent.GuardedBy;

/**
 * Factory for BookKeeperLogs.
 */
@Slf4j
public class BookKeeperLogFactory implements DurableDataLogFactory {
    //region Members

    // Period of inspection to meet the maximum number of log creation attempts for a given container.
    private static final Duration LOG_CREATION_INSPECTION_PERIOD = Duration.ofSeconds(60);
    // Maximum number of log creation attempts for a given container before considering resetting the BK client.
    private static final int MAX_CREATE_ATTEMPTS_PER_LOG = 2;

    private final String namespace;
    private final CuratorFramework zkClient;
    private final AtomicReference<BookKeeper> bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executor;
    @GuardedBy("this")
    private final Map<Integer, LogInitializationRecord> logInitializationTracker = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<Timer> lastBookkeeperClientReset = new AtomicReference<>(new Timer());

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
        tryResetBookkeeperClient(logId);
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
        tryResetBookkeeperClient(logId);
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
                .setClientTcpNoDelay(true)
                .setAddEntryTimeout(writeTimeout)
                .setReadEntryTimeout(readTimeout)
                .setGetBookieInfoTimeout(readTimeout)
                .setEnableDigestTypeAutodetection(true)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());

        if (this.config.isTLSEnabled()) {
            config = (ClientConfiguration) config.setTLSProvider("OpenSSL");
            config = config.setTLSTrustStore(this.config.getTlsTrustStore());
            config.setTLSTrustStorePasswordPath(this.config.getTlsTrustStorePasswordPath());
        }

        String metadataServiceUri = "zk://" + this.config.getZkAddress();
        if (this.config.getBkLedgerPath().isEmpty()) {
            metadataServiceUri += "/" + this.namespace + "/bookkeeper/ledgers";
        } else {
            metadataServiceUri += this.config.getBkLedgerPath();
        }
        config = config.setMetadataServiceUri(metadataServiceUri);

        if (this.config.isEnforceMinNumRacksPerWriteQuorum()) {
            config = config.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
            config.setEnforceMinNumRacksPerWriteQuorum(this.config.isEnforceMinNumRacksPerWriteQuorum());
            config.setMinNumRacksPerWriteQuorum(this.config.getMinNumRacksPerWriteQuorum());
            config.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY, this.config.getNetworkTopologyFileName());
        }

        return BookKeeper.newBuilder(config)
                         .build();
    }

    /**
     * Recreate the Bookkeeper client if a given log exhibits MAX_CREATE_ATTEMPTS_PER_LOG creation attempts (as a proxy
     * for Container recoveries) within the period of time defined in LOG_CREATION_INSPECTION_PERIOD.
     *
     * @param logId Id of the log being restarted.
     */
    private void tryResetBookkeeperClient(int logId) {
        synchronized (this) {
            LogInitializationRecord record = logInitializationTracker.get(logId);
            if (record != null) {
                // Account for a restart of the Bookkeeper log.
                record.incrementLogCreations();
                // If the number of restarts for a single container is meets the threshold, let's reset the BK client.
                if (record.isBookkeeperClientResetNeeded()
                        && lastBookkeeperClientReset.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                    try {
                        log.info("Start creating Bookkeeper client in reset.");
                        BookKeeper newClient = startBookKeeperClient();
                        // If we have been able to create a new client successfully, reset the current one and update timer.
                        log.info("Successfully created new Bookkeeper client, setting it as the new one to use.");
                        BookKeeper oldClient = this.bookKeeper.getAndSet(newClient);
                        lastBookkeeperClientReset.set(new Timer());
                        // Lastly, attempt to close the old client.
                        log.info("Attempting to close old client.");
                        oldClient.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Failure resetting the Bookkeeper client", e);
                    }
                }
            } else {
                logInitializationTracker.put(logId, new LogInitializationRecord());
            }
        }
    }

    @VisibleForTesting
    public Map<Integer, LogInitializationRecord> getLogInitializationTracker() {
        return logInitializationTracker;
    }

    @VisibleForTesting
    public void setLastBookkeeperClientReset(Timer timer) {
        lastBookkeeperClientReset.set(timer);
    }

    //endregion

    /**
     * Keeps track of the number of log creation attempts within an inspection period.
     */
    static class LogInitializationRecord {
        private final AtomicReference<Timer> timer = new AtomicReference<>(new Timer());
        private final AtomicInteger counter = new AtomicInteger(0);

        /**
         * Returns whether the Bookkeeper client should be reset based on the max allowed attempts of re-creating a
         * log within the inspection period.
         *
         * @return whether to re-create the Bookkeeper client or not.
         */
        boolean isBookkeeperClientResetNeeded() {
            return timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) < 0 && counter.get() >= MAX_CREATE_ATTEMPTS_PER_LOG;
        }

        /**
         * Increments the counter for log restarts within a particular inspection period. If the las sample is older
         * than the inspection period, the timer and the counter are reset.
         */
        void incrementLogCreations() {
            // If the time since the last log creation is too far, we need to refresh the timer to the new inspection
            // period and set the counter of log creations to 1.
            if (timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                timer.set(new Timer());
                counter.set(1);
            } else {
                // Otherwise, just increment the counter.
                counter.incrementAndGet();
            }
        }
    }
}
