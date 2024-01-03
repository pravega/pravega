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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.storage.DataLogCorruptedException;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogTestBase;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteFailureException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for BookKeeperLog. These require that a compiled BookKeeper distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public abstract class BookKeeperLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT = 500;
    private static final int BOOKIE_COUNT = 1;
    private static final int THREAD_POOL_SIZE = 3;
    private static final int MAX_WRITE_ATTEMPTS = 5;
    private static final int MAX_LEDGER_SIZE = WRITE_MAX_LENGTH * Math.max(10, WRITE_COUNT / 20);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(100);
    private final AtomicReference<BookKeeperConfig> config = new AtomicReference<>();
    private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private final AtomicReference<BookKeeperLogFactory> factory = new AtomicReference<>();
    private final AtomicBoolean secureBk = new AtomicBoolean();
    private final AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();

    /**
     * Start BookKeeper once for the duration of this class. This is pretty strenuous, so in the interest of running time
     * we only do it once.
     */
    public void setUpBookKeeper(boolean secure) throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        secureBk.set(secure);
        String testId = Long.toHexString(System.nanoTime());
        int zkPort = TestUtils.getAvailableListenPort();
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        String ledgersPath = "/pravega/bookkeeper/ledgers/" + testId;
        val runner = BookKeeperServiceRunner.builder()
                                            .startZk(true)
                                            .zkPort(zkPort)
                                            .ledgersPath(ledgersPath)
                                            .secureBK(isSecure())
                                            .secureZK(isSecure())
                                            .tlsTrustStore("../../../config/bookie.truststore.jks")
                                            .tLSKeyStore("../../../config/bookie.keystore.jks")
                                            .tLSKeyStorePasswordPath("../../../config/bookie.keystore.jks.passwd")
                                            .bookiePorts(bookiePorts)
                                            .build();
        runner.startAll();
        bkService.set(runner);
        
        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + testId;
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("127.0.0.1:" + zkPort)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                .build());
        this.zkClient.get().start();
        this.zkClient.get().blockUntilConnected();

        // Setup config to use the port and namespace.
        this.config.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "127.0.0.1:" + zkPort)
                .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, MAX_LEDGER_SIZE)
                .with(BookKeeperConfig.BK_DIGEST_TYPE, DigestType.DUMMY.name())
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, ledgersPath)
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_TLS_ENABLED, isSecure())
                .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 5000)
                .build());

        // Create default factory.
        val factory = new BookKeeperLogFactory(this.config.get(), this.zkClient.get(), executorService());
        factory.initialize();
        this.factory.set(factory);        
    }

    public boolean isSecure() {
        return secureBk.get();
    }

    @After
    public void tearDownBookKeeper() throws Exception {
        val process = bkService.getAndSet(null);
        if (process != null) {
            process.close();
        }
        
        val factory = this.factory.getAndSet(null);
        if (factory != null) {
            factory.close();
        }

        val zkClient = this.zkClient.getAndSet(null);
        if (zkClient != null) {
            zkClient.close();
        }
    }


    /**
     * Tests the BookKeeperLogFactory and its initialization.
     */
    @Test
    public void testFactoryInitialize() {
        BookKeeperConfig bkConfig = BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "127.0.0.1:" + TestUtils.getAvailableListenPort())
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, WRITE_MAX_LENGTH * 10) // Very frequent rollovers.
                .with(BookKeeperConfig.ZK_METADATA_PATH, this.zkClient.get().getNamespace())
                .build();
        @Cleanup
        val factory = new BookKeeperLogFactory(bkConfig, this.zkClient.get(), executorService());
        AssertExtensions.assertThrows("",
                factory::initialize,
                ex -> ex instanceof DataLogNotAvailableException &&
                        ex.getCause() != null
        );
    }

    /**
     * Tests the ability to auto-close upon a permanent write failure caused by BookKeeper.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testAutoCloseOnBookieFailure() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            try {
                // Suspend a bookie (this will trigger write errors).
                stopFirstBookie();

                // First write should fail. Either a DataLogNotAvailableException (insufficient bookies) or
                // WriteFailureException (general unable to write) should be thrown.
                AssertExtensions.assertSuppliedFutureThrows(
                        "First write did not fail with the appropriate exception.",
                        () -> log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT),
                        ex -> ex instanceof RetriesExhaustedException
                                && (ex.getCause() instanceof DataLogNotAvailableException
                                || isLedgerClosedException(ex.getCause()))
                                || ex instanceof ObjectClosedException
                                || ex instanceof CancellationException);

                // Subsequent writes should be rejected since the BookKeeperLog is now closed.
                AssertExtensions.assertSuppliedFutureThrows(
                        "Second write did not fail with the appropriate exception.",
                        () -> log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT),
                        ex -> ex instanceof ObjectClosedException
                                || ex instanceof CancellationException);
            } finally {
                // Don't forget to resume the bookie.
                restartFirstBookie();
            }
        }
    }

    /**
     * Tests the ability to retry writes when Bookies fail.
     */
    @Test
    public void testAppendTransientBookieFailure() throws Exception {
        TreeMap<LogAddress, byte[]> writeData = new TreeMap<>(Comparator.comparingLong(LogAddress::getSequence));
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            val dataList = new ArrayList<byte[]>();
            val futures = new ArrayList<CompletableFuture<LogAddress>>();

            try {
                // Suspend a bookie (this will trigger write errors).
                suspendFirstBookie();

                // Issue appends in parallel, without waiting for them.
                int writeCount = getWriteCount();
                for (int i = 0; i < writeCount; i++) {
                    byte[] data = getWriteData();
                    futures.add(log.append(new CompositeByteArraySegment(data), TIMEOUT));
                    dataList.add(data);
                }
            } finally {
                // Resume the bookie with the appends still in flight.
                resumeFirstBookie();
                AssertExtensions.assertThrows("Bookies should be running, but they aren't",
                        this::restartFirstBookie, ex -> ex instanceof IllegalStateException);
            }

            // Wait for all writes to complete, then reassemble the data in the order set by LogAddress.
            val addresses = Futures.allOfWithResults(futures).join();
            for (int i = 0; i < dataList.size(); i++) {
                writeData.put(addresses.get(i), dataList.get(i));
            }
        }

        // Verify data.
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            verifyReads(log, writeData);
        }
    }

    /**
     * Tests the ability to retry writes when Bookies fail.
     */
    @Test
    public void testAppendPermanentFailures() throws Exception {
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            List<CompletableFuture<LogAddress>> appendFutures = new ArrayList<>();
            try {
                // Suspend a bookie (this will trigger write errors).
                stopFirstBookie();

                // Issue appends in parallel.
                int writeCount = getWriteCount();
                for (int i = 0; i < writeCount; i++) {
                    appendFutures.add(log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT));
                }

                // Verify that all writes failed or got cancelled.
                AtomicBoolean cancellationEncountered = new AtomicBoolean(false);
                for (val f: appendFutures) {
                    AssertExtensions.assertThrows(
                            "Write did not fail correctly.",
                            () -> f.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                            ex -> {
                                cancellationEncountered.set(cancellationEncountered.get() || ex instanceof ObjectClosedException);
                                if (cancellationEncountered.get()) {
                                    return ex instanceof ObjectClosedException;
                                } else {
                                    return ex instanceof RetriesExhaustedException
                                            || ex instanceof DurableDataLogException;
                                }
                            });
                }
                AssertExtensions.assertThrows("Bookies shouldn't be running, but they are",
                        this::suspendFirstBookie, ex -> ex instanceof IllegalStateException);
                AssertExtensions.assertThrows("Bookies shouldn't be running, but they are",
                        this::resumeFirstBookie, ex -> ex instanceof IllegalStateException);
            } finally {
                // Don't forget to resume the bookie, but only AFTER we are done testing.
                restartFirstBookie();
            }
        }
    }

    /**
     * Tests the ability of BookKeeperLog to automatically remove empty ledgers during initialization.
     */
    @Test
    public void testRemoveEmptyLedgers() throws Exception {
        final int count = 100;
        final int writeEvery = count / 10;
        final Predicate<Integer> shouldAppendAnything = i -> i % writeEvery == 0;
        val allLedgers = new ArrayList<Map.Entry<Long, LedgerMetadata.Status>>();
        final Predicate<Integer> shouldExist = index -> (index >= allLedgers.size() - Ledgers.MIN_FENCE_LEDGER_COUNT)
                || (allLedgers.get(index).getValue() != LedgerMetadata.Status.Empty);

        for (int i = 0; i < count; i++) {
            try (BookKeeperLog log = (BookKeeperLog) createDurableDataLog()) {
                log.initialize(TIMEOUT);

                boolean shouldAppend = shouldAppendAnything.test(i);
                val currentMetadata = log.loadMetadata();
                val lastLedger = currentMetadata.getLedgers().get(currentMetadata.getLedgers().size() - 1);
                allLedgers.add(new AbstractMap.SimpleImmutableEntry<>(lastLedger.getLedgerId(),
                        shouldAppend ? LedgerMetadata.Status.NotEmpty : LedgerMetadata.Status.Empty));
                val metadataLedgers = currentMetadata.getLedgers().stream().map(LedgerMetadata::getLedgerId).collect(Collectors.toSet());

                // Verify Log Metadata does not contain old empty ledgers.
                for (int j = 0; j < allLedgers.size(); j++) {
                    val e = allLedgers.get(j);
                    val expectedExist = shouldExist.test(j);
                    Assert.assertEquals("Unexpected state for metadata. AllLedgerCount=" + allLedgers.size() +
                                    ", LedgerIndex=" + j + ", LedgerStatus=" + e.getValue(),
                            expectedExist, metadataLedgers.contains(e.getKey()));
                }

                // Append some data to this Ledger, if needed.
                if (shouldAppend) {
                    log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }

        // Verify that these ledgers have also been deleted from BookKeeper.
        for (int i = 0; i < allLedgers.size(); i++) {
            val e = allLedgers.get(i);
            if (shouldExist.test(i)) {
                // This should not throw any exceptions.
                Ledgers.openFence(e.getKey(), this.factory.get().getBookKeeperClient(), this.config.get());
            } else {
                AssertExtensions.assertThrows(
                        "Ledger not deleted from BookKeeper.",
                        () -> Ledgers.openFence(e.getKey(), this.factory.get().getBookKeeperClient(), this.config.get()),
                        ex -> true);
            }
        }
    }

    /**
     * Verifies that {@link BookKeeperLog#initialize} is able to handle the situation when a Ledger is marked as Empty
     * but it is also deleted from BookKeeper. This ledger should be ignored and not cause the initialization to fail.
     */
    @Test
    public void testMissingEmptyLedgers() throws Exception {
        final int count = 10;
        final int writeEvery = 5; // Every 10th Ledger has data.
        final Predicate<Integer> shouldAppendAnything = i -> i % writeEvery == 0;

        val currentMetadata = new AtomicReference<LogMetadata>();
        for (int i = 0; i < count; i++) {
            boolean isEmpty = !shouldAppendAnything.test(i);
            //boolean isDeleted = shouldDelete.test(i);
            try (BookKeeperLog log = (BookKeeperLog) createDurableDataLog()) {
                log.initialize(TIMEOUT);
                currentMetadata.set(log.loadMetadata());

                // Delete the last Empty ledger, if any.
                val toDelete = Lists.reverse(currentMetadata.get().getLedgers()).stream()
                        .filter(m -> m.getStatus() == LedgerMetadata.Status.Empty)
                        .findFirst().orElse(null);
                if (toDelete != null) {
                    Ledgers.delete(toDelete.getLedgerId(), this.factory.get().getBookKeeperClient());
                }

                // Append some data to this Ledger, if needed - this will mark it as NotEmpty.
                if (!isEmpty) {
                    log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }

        // Now try to delete a valid ledger and verify that an exception was actually thrown.
        val validLedgerToDelete = Lists.reverse(currentMetadata.get().getLedgers()).stream()
                .filter(m -> m.getStatus() != LedgerMetadata.Status.Empty)
                .findFirst().orElse(null);
        if (validLedgerToDelete != null) {
            Ledgers.delete(validLedgerToDelete.getLedgerId(), this.factory.get().getBookKeeperClient());
        }

        AssertExtensions.assertThrows(
                "No exception thrown if valid ledger was deleted.",
                () -> {
                    @Cleanup
                    val log = createDurableDataLog();
                    log.initialize(TIMEOUT);
                },
                ex -> ex instanceof DurableDataLogException && ex.getCause() instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException);
    }

    @Test
    public void testWriteSettings() {
        @Cleanup
        val log = createDurableDataLog();
        val ws = log.getWriteSettings();
        Assert.assertEquals(BookKeeperConfig.MAX_APPEND_LENGTH, ws.getMaxWriteLength());
        Assert.assertEquals(this.config.get().getMaxOutstandingBytes(), ws.getMaxOutstandingBytes());
    }

    @Test
    public void testReconcileMetadata() throws Exception {
        @Cleanup
        BookKeeperAdmin a = new BookKeeperAdmin((org.apache.bookkeeper.client.BookKeeper) this.factory.get().getBookKeeperClient());
        val initialLedgers = Sets.newHashSet(a.listLedgers());

        // Test initialization (node creation).
        try (val log = new TestBookKeeperLog()) {
            // Data not persisted and we throw an error - this is a real fencing event.
            log.setThrowZkException(true);
            log.setPersistData(false);
            AssertExtensions.assertThrows(
                    "Create(Persist=False, Throw=True)",
                    () -> log.initialize(TIMEOUT),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);
            Assert.assertEquals(1, log.getCreateExceptionCount());

            // Data persisted correctly and we throw an error - reconciliation needed.
            log.setPersistData(true);
            log.initialize(TIMEOUT);
            Assert.assertEquals("Create(Persist=True, Throw=True)", 2, log.getCreateExceptionCount());
        }

        val expectedLedgerIds = new HashSet<Long>();
        // Test updates (subsequent recoveries).
        try (val log = new TestBookKeeperLog()) {
            // Data not persisted and we throw an error - this is a real fencing event.
            log.setThrowZkException(true);
            log.setPersistData(false);
            AssertExtensions.assertThrows(
                    "Update(Persist=False, Throw=True)",
                    () -> log.initialize(TIMEOUT),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);
            Assert.assertEquals(1, log.getUpdateExceptionCount());

            // Data persisted correctly and we throw an error - reconciliation needed.
            log.setPersistData(true);
            log.initialize(TIMEOUT);
            Assert.assertEquals("Update(Persist=True, Throw=True)", 2, log.getUpdateExceptionCount());
            log.loadMetadata().getLedgers().stream().map(LedgerMetadata::getLedgerId).forEach(expectedLedgerIds::add);
        }

        // Verify ledger cleanup.
        val allLedgers = Sets.newHashSet(a.listLedgers());
        allLedgers.removeAll(initialLedgers);
        Assert.assertEquals("Unexpected ledgers in BK.", expectedLedgerIds, allLedgers);
    }

    /**
     * Tests {@link BookKeeperLogFactory#createDebugLogWrapper}.
     */
    @Test
    public void testDebugLogWrapper() throws Exception {
        @Cleanup
        val wrapper = this.factory.get().createDebugLogWrapper(0);
        val readOnly = wrapper.asReadOnly();
        val writeSettings = readOnly.getWriteSettings();
        Assert.assertEquals(BookKeeperConfig.MAX_APPEND_LENGTH, writeSettings.getMaxWriteLength());
        Assert.assertEquals((int) BookKeeperConfig.BK_WRITE_TIMEOUT.getDefaultValue(), writeSettings.getMaxWriteTimeout().toMillis());
        Assert.assertEquals((int) BookKeeperConfig.MAX_OUTSTANDING_BYTES.getDefaultValue(), writeSettings.getMaxOutstandingBytes());
        AssertExtensions.assertThrows(
                "registerQueueStateChangeListener should not be implemented.",
                () -> readOnly.registerQueueStateChangeListener(new ThrottleSourceListener() {
                    @Override
                    public void notifyThrottleSourceChanged() {

                    }

                    @Override
                    public boolean isClosed() {
                        return false;
                    }
                }),
                ex -> ex instanceof UnsupportedOperationException);
    }

    /**
     * Tests {@link DebugBookKeeperLogWrapper#reconcileLedgers}.
     */
    @Test
    public void testReconcileLedgers() throws Exception {
        final int initialLedgerCount = 5;
        final int midPoint = initialLedgerCount / 2;
        final BookKeeper bk = this.factory.get().getBookKeeperClient();
        long sameLogSmallIdLedger = -1; // BK Ledger that belongs to this log, but has small id.

        // Create a Log and add a few ledgers.
        for (int i = 0; i < initialLedgerCount; i++) {
            try (BookKeeperLog log = (BookKeeperLog) createDurableDataLog()) {
                log.initialize(TIMEOUT);
                log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            if (i == midPoint) {
                // We need to create this ledger now. We have no control over the assigned Ledger Id, but we are guaranteed
                // for them to be generated monotonically increasing.
                sameLogSmallIdLedger = createAndCloseLedger(CONTAINER_ID, false);
            }
        }

        // Verify we cannot do this while the log is enabled. This also helps us with getting the final list of ledgers
        // before reconciliation.
        BookKeeperLog log = (BookKeeperLog) createDurableDataLog();
        val wrapper = this.factory.get().createDebugLogWrapper(log.getLogId());
        AssertExtensions.assertThrows(
                "reconcileLedgers worked with non-disabled log.",
                () -> wrapper.reconcileLedgers(Collections.emptyList()),
                ex -> ex instanceof IllegalStateException);

        wrapper.disable();
        val initialMetadata = wrapper.fetchMetadata();
        val expectedLedgers = new ArrayList<>(initialMetadata.getLedgers());

        // The last ledger has been created as part of our last log creation. It will be empty and thus will be removed.
        expectedLedgers.remove(expectedLedgers.size() - 1);

        // Simulate the deletion of one of those ledgers.
        final LedgerMetadata deletedLedger = expectedLedgers.get(midPoint);
        expectedLedgers.remove(deletedLedger);

        // Add remaining (valid) ledgers to candidate list.
        val candidateLedgers = new ArrayList<ReadHandle>();
        for (val lm : expectedLedgers) {
            candidateLedgers.add(Ledgers.openFence(lm.getLedgerId(), bk, this.config.get()));
        }

        candidateLedgers.add(Ledgers.openFence(sameLogSmallIdLedger, bk, this.config.get()));

        // Create a ledger that has a high id, but belongs to a different log.
        long differentLogLedger = createAndCloseLedger(CONTAINER_ID + 1, false);
        candidateLedgers.add(Ledgers.openFence(differentLogLedger, bk, this.config.get()));

        // Create a BK ledger that belongs to this log, with high id, but empty.
        long sameLogHighIdEmptyLedger = createAndCloseLedger(CONTAINER_ID, true);
        candidateLedgers.add(Ledgers.openFence(sameLogHighIdEmptyLedger, bk, this.config.get()));

        // Create a BK ledger that belongs to this log, with high id, and not empty. This is the only one expected to be
        // added.
        long sameLogHighIdNonEmptyLedger = createAndCloseLedger(CONTAINER_ID, false);
        expectedLedgers.add(new LedgerMetadata(sameLogHighIdNonEmptyLedger, expectedLedgers.get(expectedLedgers.size() - 1).getSequence() + 1));
        candidateLedgers.add(Ledgers.openFence(sameLogHighIdNonEmptyLedger, bk, this.config.get()));

        // Perform reconciliation.
        boolean isChanged = wrapper.reconcileLedgers(candidateLedgers);
        Assert.assertTrue("Expected first reconcileLedgers to have changed something.", isChanged);

        isChanged = wrapper.reconcileLedgers(candidateLedgers);
        Assert.assertFalse("No expecting second reconcileLedgers to have changed anything.", isChanged);

        // Validate new metadata.
        val newMetadata = wrapper.fetchMetadata();
        Assert.assertFalse("Expected metadata to still be disabled..", newMetadata.isEnabled());
        Assert.assertEquals("Expected epoch to increase.", initialMetadata.getEpoch() + 1, newMetadata.getEpoch());
        Assert.assertEquals("Expected update version to increase.", initialMetadata.getUpdateVersion() + 1, newMetadata.getUpdateVersion());
        Assert.assertEquals("Not expected truncation address to change.", initialMetadata.getTruncationAddress(), newMetadata.getTruncationAddress());
        AssertExtensions.assertListEquals("Unexpected ledger list.", expectedLedgers, newMetadata.getLedgers(),
                (e, a) -> e.getLedgerId() == a.getLedgerId() && e.getSequence() == a.getSequence());

        checkLogReadAfterReconciliation(expectedLedgers.size());
    }

    /**
     * Tests {@link DebugBookKeeperLogWrapper#reconcileLedgers} with an empty log metadata and various types of candidate ledgers
     * that may or may not belong to it.
     */
    @Test
    public void testReconcileLedgersEmptyLog() throws Exception {
        final BookKeeper bk = this.factory.get().getBookKeeperClient();
        BookKeeperLog log = (BookKeeperLog) createDurableDataLog();
        val wrapper = this.factory.get().createDebugLogWrapper(log.getLogId());
        wrapper.disable();

        //Empty out the log's metadata.
        val emptyMetadata = LogMetadata
                .builder()
                .enabled(false)
                .epoch(1)
                .truncationAddress(LogMetadata.INITIAL_TRUNCATION_ADDRESS)
                .updateVersion(wrapper.fetchMetadata().getUpdateVersion())
                .ledgers(Collections.emptyList())
                .build();
        log.overWriteMetadata(emptyMetadata);

        // Create a few ledgers. One without an Id, one with a bad id, and one with
        WriteHandle ledgerNoLogId = createCustomLedger(null);
        val corruptedId = new HashMap<>(Ledgers.createLedgerCustomMetadata(log.getLogId()));
        corruptedId.put(Ledgers.PROPERTY_LOG_ID, "abc".getBytes());
        WriteHandle ledgerBadLogId = createCustomLedger(corruptedId);
        WriteHandle ledgerOtherLogId = createCustomLedger(Ledgers.createLedgerCustomMetadata(log.getLogId() + 1));
        WriteHandle ledgerGoodLogId = Ledgers.create(bk, this.config.get(), log.getLogId());
        List<WriteHandle> candidateLedgers = Arrays.asList(ledgerGoodLogId, ledgerBadLogId, ledgerNoLogId, ledgerOtherLogId);
        for (WriteHandle lh : candidateLedgers) {
            lh.append(new byte[100]);
        }

        // Perform reconciliation.
        boolean isChanged = wrapper.reconcileLedgers(candidateLedgers);
        Assert.assertTrue("Expected something to change.", isChanged);

        // Validate new metadata.
        val newMetadata = wrapper.fetchMetadata();
        val expectedLedgers = Collections.singletonList(ledgerGoodLogId.getId());
        val newLedgers = newMetadata.getLedgers().stream().map(LedgerMetadata::getLedgerId).collect(Collectors.toList());
        Assert.assertFalse("Expected metadata to still be disabled.", newMetadata.isEnabled());
        Assert.assertEquals("Unexpected epoch.", emptyMetadata.getUpdateVersion(), newMetadata.getEpoch());
        Assert.assertEquals("Unexpected update version.", emptyMetadata.getUpdateVersion() + 1, newMetadata.getUpdateVersion());
        AssertExtensions.assertListEquals("Unexpected ledger list.", expectedLedgers, newLedgers, Long::equals);

        checkLogReadAfterReconciliation(expectedLedgers.size());
    }

    /**
     * Tests {@link DebugBookKeeperLogWrapper#reconcileLedgers} by providing it with a few bad candidates, which should be excluded.
     */
    @Test
    public void testReconcileLedgersBadCandidates() throws Exception {
        final BookKeeper bk = this.factory.get().getBookKeeperClient();
        BookKeeperLog log = (BookKeeperLog) createDurableDataLog();
        val wrapper = this.factory.get().createDebugLogWrapper(log.getLogId());
        wrapper.disable();

        // Create a few ledgers. One without an Id, one with a bad id, and one with
        val ledgerNoProperties = createCustomLedger(null);
        Assert.assertEquals(Ledgers.NO_LOG_ID, Ledgers.getBookKeeperLogId(ledgerNoProperties));
        val noLogId = new HashMap<>(Ledgers.createLedgerCustomMetadata(log.getLogId()));
        noLogId.remove(Ledgers.PROPERTY_LOG_ID);
        val ledgerNoLogId = createCustomLedger(noLogId);
        Assert.assertEquals(Ledgers.NO_LOG_ID, Ledgers.getBookKeeperLogId(ledgerNoLogId));
        val corruptedId = new HashMap<>(Ledgers.createLedgerCustomMetadata(log.getLogId()));
        corruptedId.put(Ledgers.PROPERTY_LOG_ID, "abc".getBytes());
        val ledgerBadLogId = createCustomLedger(corruptedId);
        Assert.assertEquals(Ledgers.NO_LOG_ID, Ledgers.getBookKeeperLogId(ledgerBadLogId));
        val ledgerOtherLogId = createCustomLedger(Ledgers.createLedgerCustomMetadata(log.getLogId() + 1));
        val ledgerGoodLogId = Ledgers.create(bk, this.config.get(), log.getLogId());
        val candidateLedgers = Arrays.asList(ledgerGoodLogId, ledgerBadLogId, ledgerNoProperties, ledgerNoLogId, ledgerOtherLogId);
        for (val lh : candidateLedgers) {
            lh.append(new byte[100]);
        }

        // Perform reconciliation.
        boolean isChanged = wrapper.reconcileLedgers(candidateLedgers);
        Assert.assertTrue("Expected something to change.", isChanged);

        // Validate new metadata.
        val newMetadata = wrapper.fetchMetadata();
        val expectedLedgers = Collections.singletonList(ledgerGoodLogId.getId());
        val newLedgers = newMetadata.getLedgers().stream().map(LedgerMetadata::getLedgerId).collect(Collectors.toList());
        Assert.assertFalse("Expected metadata to still be disabled.", newMetadata.isEnabled());
        Assert.assertEquals("Unexpected epoch.", 2, newMetadata.getEpoch());
        Assert.assertEquals("Unexpected update version.", 2, newMetadata.getUpdateVersion());
        AssertExtensions.assertListEquals("Unexpected ledger list.", expectedLedgers, newLedgers, Long::equals);

        checkLogReadAfterReconciliation(expectedLedgers.size());
    }

    /**
     * Tests the ability to reject reading from a Log if it has been found it contains Ledgers that do not belong to it.
     */
    @Test
    public void testReadWithBadLogId() throws Exception {
        final BookKeeper bk = this.factory.get().getBookKeeperClient();
        try (val log = (BookKeeperLog) createDurableDataLog()) {
            log.initialize(TIMEOUT);
            log.append(new CompositeByteArraySegment(new byte[100]), TIMEOUT).join();
        }

        // Add some bad ledgers to the log's metadata.
        val writeLog = (BookKeeperLog) createDurableDataLog();
        val wrapper = this.factory.get().createDebugLogWrapper(writeLog.getLogId());
        wrapper.disable();

        val ledgerNoLogId = createCustomLedger(null);
        val corruptedId = new HashMap<>(Ledgers.createLedgerCustomMetadata(CONTAINER_ID));
        corruptedId.put(Ledgers.PROPERTY_LOG_ID, "abc".getBytes());
        val ledgerBadLogId = createCustomLedger(corruptedId);
        val ledgerOtherLogId = createCustomLedger(Ledgers.createLedgerCustomMetadata(CONTAINER_ID + 1));
        val ledgerGoodLogId = Ledgers.create(bk, this.config.get(), CONTAINER_ID);
        val candidateLedgers = Arrays.asList(ledgerNoLogId, ledgerBadLogId, ledgerOtherLogId, ledgerGoodLogId);
        for (val lh : candidateLedgers) {
            lh.append(new byte[100]);
        }

        // Persist the metadata with bad ledgers.
        val initialMetadata = wrapper.fetchMetadata();
        val newLedgers = new ArrayList<LedgerMetadata>();
        newLedgers.addAll(initialMetadata.getLedgers());
        candidateLedgers.forEach(l -> newLedgers.add(new LedgerMetadata(l.getId(), newLedgers.size() + 1)));
        val badMetadata = LogMetadata
                .builder()
                .enabled(false)
                .epoch(initialMetadata.getEpoch() + 1)
                .truncationAddress(LogMetadata.INITIAL_TRUNCATION_ADDRESS)
                .updateVersion(wrapper.fetchMetadata().getUpdateVersion())
                .ledgers(newLedgers)
                .build();
        writeLog.overWriteMetadata(badMetadata);

        // Perform an initial read. This should fail.
        val readLog = (BookKeeperLog) createDurableDataLog();
        readLog.enable();
        readLog.initialize(TIMEOUT);
        @Cleanup
        val reader = readLog.getReader();
        AssertExtensions.assertThrows(
                "No read exception thrown.",
                () -> {
                    while (reader.getNext() != null) {
                        // This is intentionally left blank. We do not care what we read back.
                    }
                },
                ex -> ex instanceof DataLogCorruptedException);

        // Perform reconciliation.
        BookKeeperLog reconcileLog = (BookKeeperLog) createDurableDataLog();
        DebugBookKeeperLogWrapper reconcileWrapper = this.factory.get().createDebugLogWrapper(reconcileLog.getLogId());
        reconcileWrapper.disable();
        val allLedgers = new ArrayList<ReadHandle>();
        for (LedgerMetadata lm : reconcileWrapper.fetchMetadata().getLedgers()) {
            allLedgers.add(reconcileWrapper.openLedgerNoFencing(lm));
        }

        boolean isChanged = reconcileWrapper.reconcileLedgers(allLedgers);
        Assert.assertTrue("Expected something to change.", isChanged);

        // There should only be two ledgers that survived: the one where we wrote to the original log and ledgerGoodLogId.
        checkLogReadAfterReconciliation(2);
    }

    private void checkLogReadAfterReconciliation(int expectedLedgerCount) throws Exception {
        val newLog = (BookKeeperLog) createDurableDataLog();
        newLog.enable();
        newLog.initialize(TIMEOUT);
        @Cleanup
        val reader = newLog.getReader();
        DurableDataLog.ReadItem ri;
        int readCount = 0;
        while ((ri = reader.getNext()) != null) {
            AssertExtensions.assertGreaterThan("Not expecting empty read.", 0, ri.getLength());
            readCount++;
        }

        Assert.assertEquals("Unexpected number of entries/ledgers read.", expectedLedgerCount, readCount);
    }

    @Test
    public void testDebugLogWrapperFactoryMethods() throws DurableDataLogException {
        Assert.assertEquals(this.factory.get().getBackupLogId(), Ledgers.BACKUP_LOG_ID);
        Assert.assertEquals(this.factory.get().getRepairLogId(), Ledgers.REPAIR_LOG_ID);
        @Cleanup
        DebugBookKeeperLogWrapper wrapper = this.factory.get().createDebugLogWrapper(0);
        AssertExtensions.assertThrows(DurableDataLogException.class, () -> wrapper.forceMetadataOverWrite(new LogMetadata(1)));
        @Cleanup
        val bkLog = this.factory.get().createDurableDataLog(0);
        bkLog.initialize(TIMEOUT);
        wrapper.forceMetadataOverWrite(new LogMetadata(1));
        Assert.assertNotNull(wrapper.fetchMetadata());
        wrapper.deleteDurableLogMetadata();
        Assert.assertNull(wrapper.fetchMetadata());
    }


    @Test
    public void testDeleteLedgersStartingWithId() throws Exception {
        final int initialLedgerCount = 5;
        for (int i = 0; i < initialLedgerCount; i++) {
            try (BookKeeperLog log = (BookKeeperLog) createDurableDataLog()) {
                log.initialize(TIMEOUT);
                log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        BookKeeperLog log = (BookKeeperLog) createDurableDataLog();
        val wrapper = this.factory.get().createDebugLogWrapper(log.getLogId());
        Assert.assertTrue(wrapper.fetchMetadata().isEnabled());
        wrapper.markAsDisabled();
        Assert.assertFalse(wrapper.fetchMetadata().isEnabled());
        wrapper.deleteLedgersStartingWithId(3);
        Assert.assertNotNull(wrapper.fetchMetadata().getLedgers().size());

        AssertExtensions.assertThrows(
                "No such ledger exist",
                () -> wrapper.deleteLedgersStartingWithId(6),
                ex -> ex instanceof DurableDataLogException);
    }

    @Test
    public void testOverrideEpochInLog() throws Exception {
        try (BookKeeperLog log = (BookKeeperLog) createDurableDataLog()) {
            log.initialize(TIMEOUT);
            log.append(new CompositeByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            val epoch = log.getEpoch();
            long epochToOverride = 100;
            log.overrideEpoch(epochToOverride);
            val overridenEpoch = log.loadMetadata();
            Assert.assertNotEquals("Epoch not overriden!", epoch, overridenEpoch);
            Assert.assertEquals( "Overriden epoch has not been set correctly.", epochToOverride, overridenEpoch.getEpoch());
        }
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    private void stopFirstBookie() {
        bkService.get().stopBookie(0);
    }

    private void suspendFirstBookie() {
        bkService.get().suspendBookie(0);
    }

    private void resumeFirstBookie() {
        bkService.get().resumeBookie(0);
    }

    @SneakyThrows
    private void restartFirstBookie() {
        bkService.get().startBookie(0);
    }

    private static boolean isLedgerClosedException(Throwable ex) {
        return ex instanceof WriteFailureException && ex.getCause() instanceof BKLedgerClosedException;
    }

    private long createAndCloseLedger(int logId, boolean empty) throws Exception {
        @Cleanup
        WriteHandle handle = Ledgers.create(this.factory.get().getBookKeeperClient(), this.config.get(), logId);
        if (!empty) {
            handle.append(new byte[100]);
        }
        return handle.getId();
    }

    @SneakyThrows
    private WriteHandle createCustomLedger(Map<String, byte[]> customMetadata) {
        return this.factory.get().getBookKeeperClient().newCreateLedgerOp()
                .withEnsembleSize(this.config.get().getBkEnsembleSize())
                .withWriteQuorumSize(this.config.get().getBkWriteQuorumSize())
                .withAckQuorumSize(this.config.get().getBkAckQuorumSize())
                .withDigestType(this.config.get().getDigestType())
                .withPassword(this.config.get().getBKPassword())
                .withCustomMetadata(customMetadata != null ? customMetadata : Collections.emptyMap())
                .execute()
                .get();
    }

    //endregion

    //region DurableDataLogTestBase implementation

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.get().createDurableDataLog(CONTAINER_ID);
    }

    @Override
    protected DurableDataLog createDurableDataLog(Object sharedContext) {
        return createDurableDataLog(); // Nothing different for shared context.
    }

    @Override
    protected Object createSharedContext() {
        return null; // No need for shared context.
    }

    @Override
    protected LogAddress createLogAddress(long seqNo) {
        return new LedgerAddress(seqNo, seqNo);
    }

    @Override
    protected int getWriteCount() {
        return WRITE_COUNT;
    }

    //endregion

    //region TestBookKeeperLog

    @Setter
    private class TestBookKeeperLog extends BookKeeperLog {
        private boolean persistData = true;
        private boolean throwZkException = false;
        private final AtomicInteger createExceptionCount = new AtomicInteger();
        private final AtomicInteger updateExceptionCount = new AtomicInteger();

        TestBookKeeperLog() {
            super(CONTAINER_ID, zkClient.get(), factory.get().getBookKeeperClient(), config.get(), executorService());
        }

        int getCreateExceptionCount() {
            return this.createExceptionCount.get();
        }

        int getUpdateExceptionCount() {
            return this.updateExceptionCount.get();
        }

        @Override
        protected Stat createZkMetadata(byte[] serializedMetadata) throws Exception {
            assert this.persistData || this.throwZkException;
            Stat result = new Stat();
            if (this.persistData) {
                result = super.createZkMetadata(serializedMetadata);
            }
            if (this.throwZkException) {
                this.createExceptionCount.incrementAndGet();
                throw new KeeperException.NodeExistsException();
            }
            return result;
        }

        @Override
        @VisibleForTesting
        protected Stat updateZkMetadata(byte[] serializedMetadata, int version) throws Exception {
            assert this.persistData || this.throwZkException;
            Stat result = new Stat();
            if (this.persistData) {
                super.updateZkMetadata(serializedMetadata, version);
            }
            if (this.throwZkException) {
                this.updateExceptionCount.incrementAndGet();
                throw new KeeperException.BadVersionException();
            }
            return result;
        }
    }

    //endregion

    //region Actual Test Implementations

    public static class SecureBookKeeperLogTests extends BookKeeperLogTests {
        @Before
        public void startUp() throws Exception {
            setUpBookKeeper(true);
        }
    }

    public static class RegularBookKeeperLogTests extends BookKeeperLogTests {
        @Before
        public void startUp() throws Exception {
            setUpBookKeeper(false);
        }
    }

    //endregion
}
