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
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for DistributedLogDataLog. These require that a compiled DistributedLog distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class DistributedLogDataLogTests extends DurableDataLogTestBase {
    //region DistributedLog Bootstrapping

    private static final int DLOG_PORT = 7000;
    private static final String DLOG_PATH = "/home/andrei/src/distributedlog";
    private static final String DLOG_NAMESPACE = "pravegatest";
    private static final String DLOG_START = DLOG_PATH + "/distributedlog-service/bin/dlog local " + DLOG_PORT;
    private static final String DLOG_CREATE_NAMESPACE = DLOG_PATH + String.format(
            "/distributedlog-service/bin/dlog admin bind -l /ledgers -s 127.0.0.1:%s -c distributedlog://127.0.0.1:%s/%s",
            DLOG_PORT, DLOG_PORT, DLOG_NAMESPACE);

    //endregion

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;
    private static final String CLIENT_ID = "UnitTest";

    private static final DistributedLogConfig CONFIG = new TestConfig()
            .withDistributedLogHost("127.0.0.1")
            .withDistributedLogPort(DLOG_PORT)
            .withDistributedLogNamespace(DLOG_NAMESPACE);

    //region Setup and Cleanup

    private final AtomicReference<Process> dlogProcess = new AtomicReference<>();
    private final AtomicReference<DistributedLogDataLogFactory> factory = new AtomicReference<>();

    @Before
    public void initializeTest() throws Exception {
        this.dlogProcess.set(Runtime.getRuntime().exec(DLOG_START));
        Process p = Runtime.getRuntime().exec(DLOG_CREATE_NAMESPACE);
        int retVal = p.waitFor();
        Assert.assertEquals("Unable to start DistributedLog process.", 0, retVal);

        val factory = new DistributedLogDataLogFactory(CLIENT_ID, CONFIG, executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void cleanupAfterTest() {
        factory.getAndSet(null).close();
        System.out.println("DLOG: Factory closed.");

        System.out.println("DLOG: Stopping.");
        dlogProcess.get().destroyForcibly();
        System.out.println("DLOG: Stopped.");
    }

    //endregion

    //region DurableDataLogTestBase implementation

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.get().createDurableDataLog(CONTAINER_ID);
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
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            // Simulate a different client trying to
            @Cleanup
            val factory = new DistributedLogDataLogFactory(CLIENT_ID + "_secondary", CONFIG, executorService());
            factory.initialize();
            AssertExtensions.assertThrows(
                    "A second log was able to acquire the exclusive write lock, even if another log held it.",
                    () -> {
                        try (DurableDataLog log2 = factory.createDurableDataLog(CONTAINER_ID)) {
                            log2.initialize(TIMEOUT);
                        }
                    },
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // Verify we can still append and read to/from the first log.
            TreeMap<LogAddress, byte[]> writeData = populate(log, getWriteCountForWrites());
            verifyReads(log, createLogAddress(-1), writeData);
        }
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
