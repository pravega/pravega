/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.distributedlog;

import io.pravega.service.storage.DataLogWriterNotPrimaryException;
import io.pravega.service.storage.DurableDataLog;
import io.pravega.service.storage.DurableDataLogTestBase;
import io.pravega.service.storage.LogAddress;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import com.twitter.distributedlog.DLSN;
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
 * Unit test for DistributedLogDataLog. These require that a compiled DistributedLog distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class DistributedLogDataLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;
    private static final String CLIENT_ID = "UnitTest";

    private static final AtomicReference<Process> DLOG_PROCESS = new AtomicReference<>();
    private static final AtomicInteger DLOG_PORT = new AtomicInteger();
    private final AtomicReference<DistributedLogConfig> config = new AtomicReference<>();
    private final AtomicReference<DistributedLogDataLogFactory> factory = new AtomicReference<>();

    /**
     * Start DistributedLog once for the duration of this class. This is pretty strenuous, and it actually starts a
     * new process, so in the interest of running time we only do it once.
     */
    @BeforeClass
    public static void setUpDistributedLog() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        DLOG_PORT.set(TestUtils.getAvailableListenPort());
        DLOG_PROCESS.set(DistributedLogStarter.startOutOfProcess(DLOG_PORT.get()));
    }

    @AfterClass
    public static void tearDownDistributedLog() throws Exception {
        val process = DLOG_PROCESS.getAndSet(null);
        if (process != null) {
            process.destroy();
        }
    }

    /**
     * Before each test, we create a new namespace; this ensures that data created from a previous test does not leak
     * into the current one (namespaces cannot be deleted (at least not through the API)).
     */
    @Before
    public void setUp() throws Exception {
        // Create a namespace.
        String namespace = "pravegatest_" + Long.toHexString(System.nanoTime());
        DistributedLogStarter.createNamespace(namespace, DLOG_PORT.get());

        // Setup config to use the port and namespace.
        this.config.set(DistributedLogConfig
                .builder()
                .with(DistributedLogConfig.HOSTNAME, DistributedLogStarter.DLOG_HOST)
                .with(DistributedLogConfig.PORT, DLOG_PORT.get())
                .with(DistributedLogConfig.NAMESPACE, namespace)
                .build());

        // Create default factory.
        val factory = new DistributedLogDataLogFactory(CLIENT_ID, this.config.get(), executorService());
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

    //endregion

    //region DurableDataLogTestBase implementation

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.get().createDurableDataLog(CONTAINER_ID);
    }

    @Override
    protected DurableDataLog createDurableDataLog(Object sharedContext) {
        return createDurableDataLog(); // Nothing different for shared context; that is stored in DistributedLog.
    }

    @Override
    protected Object createSharedContext() {
        return null; // No need for shared context; that is stored in DistributedLog.
    }

    @Override
    protected LogAddress createLogAddress(long seqNo) {
        return new DLSNAddress(seqNo, new DLSN(0, 0, 0));
    }

    @Override
    protected int getWriteCountForWrites() {
        return WRITE_COUNT_WRITES;
    }

    @Override
    protected int getWriteCountForReads() {
        return WRITE_COUNT_READS;
    }

    @Test
    @Override
    public void testExclusiveWriteLock() throws Exception {
        // Tests the ability of the DurableDataLog to enforce an exclusive writer, by only allowing one client at a time
        // to write to the same physical log.
        final long initialEpoch;
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            initialEpoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", 0, initialEpoch);

            // Simulate a different client trying to open the same log. This should not be allowed.
            @Cleanup
            val factory = new DistributedLogDataLogFactory(CLIENT_ID + "_secondary", config.get(), executorService());
            factory.initialize();
            try (DurableDataLog log2 = factory.createDurableDataLog(CONTAINER_ID)) {
                AssertExtensions.assertThrows(
                        "A second log was able to acquire the exclusive write lock, even if another log held it.",
                        () -> log2.initialize(TIMEOUT),
                        ex -> ex instanceof DataLogWriterNotPrimaryException);

                AssertExtensions.assertThrows(
                        "getEpoch() did not throw after failed initialization.",
                        log2::getEpoch,
                        ex -> ex instanceof IllegalStateException);
            }

            // Verify we can still append and read to/from the first log.
            TreeMap<LogAddress, byte[]> writeData = populate(log, getWriteCountForWrites());
            verifyReads(log, createLogAddress(-1), writeData);
        }

        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);
            long epoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on non-empty log initialization.", initialEpoch, epoch);
        }
    }

    //endregion
}
