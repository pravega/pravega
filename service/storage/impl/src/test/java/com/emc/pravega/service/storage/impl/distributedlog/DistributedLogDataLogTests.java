/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogTestBase;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.tools.Tool;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;
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

    private static final int BASE_PORT = 11000;
    private static final String DLOG_HOST = "127.0.0.1";
    private static final String DLOG_NAMESPACE = "pravegatest";
    private static final AtomicInteger CONTAINER_ID = new AtomicInteger(9999); // This changes with every run because we cannot cleanup.
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;
    private static final String CLIENT_ID = "UnitTest";

    private static final AtomicReference<DistributedLogConfig> CONFIG = new AtomicReference<>();
    private static final AtomicReference<LocalDLMEmulator> DLOG_PROCESS = new AtomicReference<>();
    private final AtomicReference<DistributedLogDataLogFactory> factory = new AtomicReference<>();

    @BeforeClass
    public static void startDistributedLog() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        final int port = BASE_PORT + new Random().nextInt(1000);

        // Start DistributedLog in-process.
        ServerConfiguration sc = new ServerConfiguration()
                .setJournalAdaptiveGroupWrites(false)
                .setJournalMaxGroupWaitMSec(1);
        val dlm = LocalDLMEmulator.newBuilder()
                                  .zkPort(port)
                                  .serverConf(sc)
                                  .build();
        dlm.start();
        DLOG_PROCESS.set(dlm);

        // Create a namespace.
        Tool tool = ReflectionUtils.newInstance(com.twitter.distributedlog.admin.DistributedLogAdmin.class.getName(), Tool.class);
        tool.run(new String[]{
                "bind",
                "-l", "/ledgers",
                "-s", String.format("%s:%s", DLOG_HOST, port),
                "-c", String.format("distributedlog://%s:%s/%s", DLOG_HOST, port, DLOG_NAMESPACE)});

        // Setup config to use the port and namespace.
        CONFIG.set(new TestConfig()
                .withDistributedLogHost(DLOG_HOST)
                .withDistributedLogPort(port)
                .withDistributedLogNamespace(DLOG_NAMESPACE));
    }

    @AfterClass
    public static void stopDistributedLog() throws Exception {
        // It turns out that this doesn't fully shut down DistributedLog; something (like ZooKeeper) is still hanging around,
        // which is why we need to start it before we run the class and shut it down at the end vs. with each test.
        DLOG_PROCESS.get().teardown();
    }

    @Before
    public void initializeTest() throws Exception {
        // Create default factory.
        val factory = new DistributedLogDataLogFactory(CLIENT_ID, CONFIG.get(), executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void cleanupAfterTest() throws Exception {
        this.factory.getAndSet(null).close();
    }

    //endregion

    //region DurableDataLogTestBase implementation

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.get().createDurableDataLog(CONTAINER_ID.getAndIncrement());
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
        final int containerId = CONTAINER_ID.getAndIncrement();
        try (DurableDataLog log = this.factory.get().createDurableDataLog(containerId)) {
            log.initialize(TIMEOUT);

            // Simulate a different client trying to
            @Cleanup
            val factory = new DistributedLogDataLogFactory(CLIENT_ID + "_secondary", CONFIG.get(), executorService());
            factory.initialize();
            AssertExtensions.assertThrows(
                    "A second log was able to acquire the exclusive write lock, even if another log held it.",
                    () -> {
                        try (DurableDataLog log2 = factory.createDurableDataLog(containerId)) {
                            log2.initialize(TIMEOUT);
                        }
                    },
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // Verify we can still append and read to/from the first log.
            TreeMap<LogAddress, byte[]> writeData = populate(log, getWriteCountForWrites());
            verifyReads(log, createLogAddress(-1), writeData);
        }
        System.out.println();
    }

    //endregion

    //region TestConfig

    private static class TestConfig extends DistributedLogConfig {
        @Getter
        private String distributedLogHost;
        @Getter
        private int distributedLogPort;
        @Getter
        private String distributedLogNamespace;

        TestConfig() throws ConfigurationException {
            super(new Properties());
        }

        TestConfig withDistributedLogHost(String value) {
            this.distributedLogHost = value;
            return this;
        }

        TestConfig withDistributedLogPort(int value) {
            this.distributedLogPort = value;
            return this;
        }

        TestConfig withDistributedLogNamespace(String value) {
            this.distributedLogNamespace = value;
            return this;
        }
    }

    //endregion
}
