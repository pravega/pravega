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

package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.ProcessStarter;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.val;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Store adapter that executes requests directly to BookKeeper via Ledgers.
 */
class BookKeeperAdapter extends StoreAdapter {
    //region Members

    private final TestConfig testConfig;
    private final BookKeeperConfig bkConfig;
    private final ScheduledExecutorService executor;
    private final ConcurrentHashMap<String, WriteHandle> ledgers;
    private final Thread stopBookKeeperProcess;
    private Process bookKeeperService;
    private CuratorFramework zkClient;
    private BookKeeperLogFactory logFactory;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeperAdapter class.
     *
     * @param testConfig The Test Configuration to use.
     * @param bkConfig   The BookKeeper Configuration to use.
     * @param executor   An Executor to use for test-related async operations.
     */
    BookKeeperAdapter(TestConfig testConfig, BookKeeperConfig bkConfig, ScheduledExecutorService executor) {
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.bkConfig = Preconditions.checkNotNull(bkConfig, "bkConfig");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(testConfig.getBookieCount() > 0, "BookKeeperAdapter requires at least one Bookie.");
        this.ledgers = new ConcurrentHashMap<>();
        this.stopBookKeeperProcess = new Thread(this::stopBookKeeper);
        Runtime.getRuntime().addShutdownHook(this.stopBookKeeperProcess);
    }

    //endregion

    //region StoreAdapter Implementation.

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.CreateStream
                || feature == Feature.Append;
    }

    @Override
    protected void startUp() throws Exception {
        // Start BookKeeper.
        this.bookKeeperService = BookKeeperAdapter.startBookKeeperOutOfProcess(this.testConfig, this.logId);

        // Create a ZK client.
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + this.testConfig.getZkPort())
                .namespace("pravega")
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .build();
        this.zkClient.start();

        // Create a BK client.
        this.logFactory = new BookKeeperLogFactory(this.bkConfig, this.zkClient, this.executor);
        this.logFactory.initialize();
    }

    @Override

    protected void shutDown() {
        for (WriteHandle lh : this.ledgers.values()) {
            try {
                lh.close();
            } catch (Exception ex) {
                System.err.println(ex);
            }
        }
        this.ledgers.clear();

        BookKeeperLogFactory lf = this.logFactory;
        if (lf != null) {
            lf.close();
            this.logFactory = null;
        }

        stopBookKeeper();
        CuratorFramework zkClient = this.zkClient;
        if (zkClient != null) {
            zkClient.close();
            this.zkClient = null;
        }

        Runtime.getRuntime().removeShutdownHook(this.stopBookKeeperProcess);
    }

    @Override
    public CompletableFuture<Void> createStream(String logName, Duration timeout) {
        ensureRunning();

        return CompletableFuture.runAsync(() -> {
            WriteHandle ledger = null;
            boolean success = false;
            try {
                ledger = getBookKeeper()
                        .newCreateLedgerOp()
                        .withEnsembleSize(this.bkConfig.getBkEnsembleSize())
                        .withWriteQuorumSize(this.bkConfig.getBkWriteQuorumSize())
                        .withAckQuorumSize(this.bkConfig.getBkAckQuorumSize())
                        .withDigestType(DigestType.CRC32C)
                        .withPassword(new byte[0])
                        .execute()
                        .get();
                this.ledgers.put(logName, ledger);
                success = true;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            } finally {
                if (!success) {
                    this.ledgers.remove(logName);
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (Exception ex) {
                            System.err.println(ex);
                        }
                    }
                }
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> append(String logName, Event event, Duration timeout) {
        ensureRunning();
        WriteHandle lh = this.ledgers.getOrDefault(logName, null);
        if (lh == null) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(logName));
        }

        ArrayView s = event.getSerialization();
        return Futures.toVoid(lh
                .appendAsync(s.array(), s.arrayOffset(), s.getLength()));
    }

    @Override
    public StoreReader createReader() {
        throw new UnsupportedOperationException("createReader() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        throw new UnsupportedOperationException("createTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("mergeTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("abortTransaction() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> sealStream(String streamName, Duration timeout) {
        throw new UnsupportedOperationException("seal() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> deleteStream(String streamName, Duration timeout) {
        throw new UnsupportedOperationException("delete() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> createTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("createTable() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> deleteTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("deleteTable() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Long> updateTableEntry(String tableName, BufferView key, BufferView value, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("updateTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<Void> removeTableEntry(String tableName, BufferView key, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("removeTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<List<BufferView>> getTableEntries(String tableName, List<BufferView> keys, Duration timeout) {
        throw new UnsupportedOperationException("getTableEntry() is not supported on " + this.logId);
    }

    @Override
    public CompletableFuture<AsyncIterator<List<Map.Entry<BufferView, BufferView>>>> iterateTableEntries(String tableName, Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return null;
    }

    //endregion

    private BookKeeper getBookKeeper() {
        return this.logFactory.getBookKeeperClient();
    }

    private void stopBookKeeper() {
        val bk = this.bookKeeperService;
        if (bk != null) {
            bk.destroyForcibly();
            log("Bookies shut down.");
            this.bookKeeperService = null;
        }
    }

    /**
     * Starts a BookKeeper (using a number of bookies) along with a ZooKeeper out-of-process.
     *
     * @param config The Test Config to use. This indicates the BK Port(s), ZK Port, as well as Bookie counts.
     * @param logId  A String to use for logging purposes.
     * @return A Process referring to the newly started Bookie process.
     * @throws IOException If an error occurred.
     */
    static Process startBookKeeperOutOfProcess(TestConfig config, String logId) throws IOException {
        int bookieCount = config.getBookieCount();
        Process p = ProcessStarter
                .forClass(BookKeeperServiceRunner.class)
                .sysProp(BookKeeperServiceRunner.PROPERTY_BASE_PORT, config.getBkPort(0))
                .sysProp(BookKeeperServiceRunner.PROPERTY_BOOKIE_COUNT, bookieCount)
                .sysProp(BookKeeperServiceRunner.PROPERTY_ZK_PORT, config.getZkPort())
                .sysProp(BookKeeperServiceRunner.PROPERTY_LEDGERS_PATH, TestConfig.BK_ZK_LEDGER_PATH)
                .sysProp(BookKeeperServiceRunner.PROPERTY_START_ZK, true)
                .sysProp(BookKeeperServiceRunner.PROPERTY_LEDGERS_DIR, config.getBookieLedgersDir())
                .stdOut(ProcessBuilder.Redirect.to(new File(config.getComponentOutLogPath("bk", 0))))
                .stdErr(ProcessBuilder.Redirect.to(new File(config.getComponentErrLogPath("bk", 0))))
                .start();
        ZooKeeperServiceRunner.waitForServerUp(config.getZkPort());
        Exceptions.handleInterrupted(() -> Thread.sleep(2000));
        TestLogger.log(logId, "Zookeeper (Port %s) and BookKeeper (Ports %s-%s) started.",
                config.getZkPort(), config.getBkPort(0), config.getBkPort(bookieCount - 1));
        return p;
    }
}
