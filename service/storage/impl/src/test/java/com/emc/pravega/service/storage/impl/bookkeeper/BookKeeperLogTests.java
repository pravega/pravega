/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogTestBase;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.TestUtils;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for BookKeeperLog. These require that a compiled BookKeeper distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class BookKeeperLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;
    private static final int BOOKIE_COUNT = 3;
    private static final int THREAD_POOL_SIZE = 5;

    private static final AtomicReference<BookKeeperServiceRunner> BK_SERVICE = new AtomicReference<>();
    private static final AtomicInteger BK_PORT = new AtomicInteger();
    private final AtomicReference<BookKeeperConfig> config = new AtomicReference<>();
    private final AtomicReference<BookKeeperLogFactory> factory = new AtomicReference<>();

    /**
     * Start BookKeeper once for the duration of this class. This is pretty strenuous, and it actually starts a
     * new process, so in the interest of running time we only do it once.
     */
    @BeforeClass
    public static void setUpBookKeeper() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        BK_PORT.set(TestUtils.getAvailableListenPort());
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        val runner = BookKeeperServiceRunner.builder()
                                            .startZk(true)
                                            .zkPort(BK_PORT.get())
                                            .bookiePorts(bookiePorts)
                                            .build();
        runner.start();
        BK_SERVICE.set(runner);
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
        // Create a namespace.
        String namespace = "/pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());

        // Setup config to use the port and namespace.
        this.config.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, WRITE_MAX_LENGTH * 10) // Very frequent rollovers.
                .with(BookKeeperConfig.ZK_NAMESPACE, namespace)
                .build());

        // Create default factory.
        val factory = new BookKeeperLogFactory(this.config.get(), executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void tearDown() throws Exception {
        val factory = this.factory.getAndSet(null);
        if (factory != null) {
            factory.close();
        }
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
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
    protected int getWriteCountForWrites() {
        return WRITE_COUNT_WRITES;
    }

    @Override
    protected int getWriteCountForReads() {
        return WRITE_COUNT_READS;
    }

    //endregion

    //region Specific tests

    @Test(timeout = TIMEOUT_MILLIS)
    @Override
    public void testExclusiveWriteLock() throws Exception {
        // Tests the ability of the DurableDataLog to enforce an exclusive writer, by only allowing one client at a time
        // to write to the same physical log.
        final long initialEpoch;
        final long secondEpoch;
        TreeMap<LogAddress, byte[]> writeData;
        try (DurableDataLog log1 = createDurableDataLog()) {
            log1.initialize(TIMEOUT);
            initialEpoch = log1.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", 0, initialEpoch);

            // Simulate a different client trying to open the same log. This should be allowed and it should fence out the first log.
            @Cleanup
            val factory = new BookKeeperLogFactory(config.get(), executorService());
            factory.initialize();
            try (DurableDataLog log2 = factory.createDurableDataLog(CONTAINER_ID)) {
                log2.initialize(TIMEOUT);
                secondEpoch = log2.getEpoch();
                AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", initialEpoch, secondEpoch);

                // Verify we cannot write to the first log.
                AssertExtensions.assertThrows(
                        "The first log was not fenced out.",
                        () -> log1.append(new ByteArrayInputStream(new byte[1]), TIMEOUT),
                        ex -> ex instanceof DataLogWriterNotPrimaryException);

                // Verify we can write to the second log.
                writeData = populate(log2, getWriteCountForWrites());
            }
        }

        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            long thirdEpoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on non-empty log initialization.", secondEpoch, thirdEpoch);
            verifyReads(log, writeData);
        }
    }

    //endregion
}