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

import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
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
import java.util.Collections;
import java.util.Comparator;
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
import lombok.SneakyThrows;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for BookKeeperLog. These require that a compiled BookKeeper distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public abstract class BookKeeperLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final AtomicBoolean SECURE_BK = new AtomicBoolean();

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT = 500;
    private static final int BOOKIE_COUNT = 1;
    private static final int THREAD_POOL_SIZE = 3;
    private static final int MAX_WRITE_ATTEMPTS = 3;
    private static final int MAX_LEDGER_SIZE = WRITE_MAX_LENGTH * Math.max(10, WRITE_COUNT / 20);

    private static final AtomicReference<BookKeeperServiceRunner> BK_SERVICE = new AtomicReference<>();
    private static final AtomicInteger BK_PORT = new AtomicInteger();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private final AtomicReference<BookKeeperConfig> config = new AtomicReference<>();
    private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private final AtomicReference<BookKeeperLogFactory> factory = new AtomicReference<>();

    /**
     * Start BookKeeper once for the duration of this class. This is pretty strenuous, so in the interest of running time
     * we only do it once.
     */
    public static void setUpBookKeeper(boolean secure) throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        SECURE_BK.set(secure);
        BK_PORT.set(TestUtils.getAvailableListenPort());
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        val runner = BookKeeperServiceRunner.builder()
                                            .startZk(true)
                                            .zkPort(BK_PORT.get())
                                            .ledgersPath("/pravega/bookkeeper/ledgers")
                                            .secureBK(isSecure())
                                            .secureZK(isSecure())
                                            .tlsTrustStore("../../../config/bookie.truststore.jks")
                                            .tLSKeyStore("../../../config/bookie.keystore.jks")
                                            .tLSKeyStorePasswordPath("../../../config/bookie.keystore.jks.passwd")
                                            .bookiePorts(bookiePorts)
                                            .build();
        runner.startAll();
        BK_SERVICE.set(runner);
    }

    public static boolean isSecure() {
        return SECURE_BK.get();
    }

    @AfterClass
    public static void tearDownBookKeeper() throws Exception {
        val process = BK_SERVICE.getAndSet(null);
        if (process != null) {
            process.close();
        }
    }

    /**
     * Before each test, we create a new namespace; this ensures that data created from a previous test does not leak
     * into the current one (namespaces cannot be deleted (at least not through the API)).
     */
    @Before
    public void setUp() throws Exception {
        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + BK_PORT.get())
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build());
        this.zkClient.get().start();

        // Setup config to use the port and namespace.
        this.config.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, MAX_LEDGER_SIZE)
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperConfig.BK_TLS_ENABLED, isSecure())
                .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                .build());

        // Create default factory.
        val factory = new BookKeeperLogFactory(this.config.get(), this.zkClient.get(), executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void tearDown() {
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
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, WRITE_MAX_LENGTH * 10) // Very frequent rollovers.
                .with(BookKeeperConfig.ZK_METADATA_PATH, this.zkClient.get().getNamespace())
                .build();
        @Cleanup
        val factory = new BookKeeperLogFactory(bkConfig, this.zkClient.get(), executorService());
        AssertExtensions.assertThrows("",
                factory::initialize,
                ex -> ex instanceof DataLogNotAvailableException &&
                        ex.getCause() instanceof BKException.ZKException
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
                        () -> log.append(new ByteArraySegment(getWriteData()), TIMEOUT),
                        ex -> ex instanceof RetriesExhaustedException
                                && (ex.getCause() instanceof DataLogNotAvailableException
                                || isLedgerClosedException(ex.getCause()))
                                || ex instanceof ObjectClosedException
                                || ex instanceof CancellationException);

                // Subsequent writes should be rejected since the BookKeeperLog is now closed.
                AssertExtensions.assertSuppliedFutureThrows(
                        "Second write did not fail with the appropriate exception.",
                        () -> log.append(new ByteArraySegment(getWriteData()), TIMEOUT),
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
                stopFirstBookie();

                // Issue appends in parallel, without waiting for them.
                int writeCount = getWriteCount();
                for (int i = 0; i < writeCount; i++) {
                    byte[] data = getWriteData();
                    futures.add(log.append(new ByteArraySegment(data), TIMEOUT));
                    dataList.add(data);
                }
            } finally {
                // Resume the bookie with the appends still in flight.
                restartFirstBookie();
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
                    appendFutures.add(log.append(new ByteArraySegment(getWriteData()), TIMEOUT));
                }

                // Verify that all writes failed or got cancelled.
                AtomicBoolean cancellationEncountered = new AtomicBoolean(false);
                for (val f: appendFutures) {
                    AssertExtensions.assertThrows(
                            "Write did not fail correctly.",
                            () -> f.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                            ex -> {
                                cancellationEncountered.set(cancellationEncountered.get() || ex instanceof CancellationException);
                                if (cancellationEncountered.get()) {
                                    return ex instanceof CancellationException;
                                } else {
                                    return ex instanceof RetriesExhaustedException
                                            || ex instanceof DurableDataLogException;
                                }
                            });
                }
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
                    log.append(new ByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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

    @Test
    public void testWriteSettings() {
        @Cleanup
        val log = createDurableDataLog();
        val ws = log.getWriteSettings();
        Assert.assertEquals(BookKeeperConfig.MAX_APPEND_LENGTH, ws.getMaxWriteLength());
        Assert.assertEquals(this.config.get().getMaxOutstandingBytes(), ws.getMaxOutstandingBytes());
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

                val currentMetadata = log.loadMetadata();
                log.append(new ByteArraySegment(getWriteData()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            if (i == midPoint) {
                // We need to create this ledger now. We have no control over the assigned Ledger Id, but we are guaranteed
                // for them to be generated monotonically increasing.
                sameLogSmallIdLedger = createAndCloseLedger(CONTAINER_ID, false);
            }
        }

        // Verify we cannot do this while the log is enabled. This also helps us with getting the final list of ledgers
        // before reconciliation.
        val log = (BookKeeperLog) createDurableDataLog();
        val wrapper = new DebugLogWrapper(log, bk, this.config.get());
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
        val candidateLedgers = new ArrayList<LedgerHandle>();
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

        // Enable the new log and read from it.
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

        Assert.assertEquals("Unexpected number of entries/ledgers read.", expectedLedgers.size(), readCount);
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    private static void stopFirstBookie() {
        BK_SERVICE.get().stopBookie(0);
    }

    @SneakyThrows
    private static void restartFirstBookie() {
        BK_SERVICE.get().startBookie(0);
    }

    private static boolean isLedgerClosedException(Throwable ex) {
        return ex instanceof WriteFailureException && ex.getCause() instanceof BKException.BKLedgerClosedException;
    }

    private long createAndCloseLedger(int logId, boolean empty) throws Exception {
        @Cleanup
        val handle = Ledgers.create(this.factory.get().getBookKeeperClient(), this.config.get(), logId);
        if (!empty) {
            handle.addEntry(new byte[100]);
        }
        return handle.getId();
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
    public static class SecureBookKeeperLogTests extends BookKeeperLogTests {
        @BeforeClass
        public static void startUp() throws Exception {
            setUpBookKeeper(true);
        }
    }

    public static class RegularBookKeeperLogTests extends BookKeeperLogTests {
        @BeforeClass
        public static void startUp() throws Exception {
            setUpBookKeeper(false);
        }
    }
}