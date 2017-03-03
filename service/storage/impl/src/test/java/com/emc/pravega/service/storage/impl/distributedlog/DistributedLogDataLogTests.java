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
import com.emc.pravega.testcommon.TestUtils;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import com.twitter.distributedlog.tools.Tool;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for DistributedLogDataLog. These require that a compiled DistributedLog distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class DistributedLogDataLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final String DLOG_HOST = "127.0.0.1";
    private static final String DLOG_NAMESPACE = "pravegatest";
    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;
    private static final String CLIENT_ID = "UnitTest";

    private final AtomicReference<DistributedLogConfig> config = new AtomicReference<>();
    private final AtomicReference<LocalDLMEmulator> dlogProcess = new AtomicReference<>();
    private final AtomicReference<DistributedLogDataLogFactory> factory = new AtomicReference<>();

    @Before
    public void initializeTest() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        final int port = TestUtils.randomPort();
        startDistributedLog(port);

        // Setup config to use the port and namespace.
        this.config.set(new TestConfig()
                .withDistributedLogHost(DLOG_HOST)
                .withDistributedLogPort(port)
                .withDistributedLogNamespace(DLOG_NAMESPACE));

        // Create default factory.
        val factory = new DistributedLogDataLogFactory(CLIENT_ID, this.config.get(), executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void cleanupAfterTest() throws Exception {
        val factory = this.factory.getAndSet(null);
        if (factory != null) {
            factory.close();
        }

        stopDistributedLog();
    }

    private void startDistributedLog(int port) throws Exception {
        // Start DistributedLog in-process.
        ServerConfiguration sc = new ServerConfiguration()
                .setJournalAdaptiveGroupWrites(false)
                .setJournalMaxGroupWaitMSec(0);
        val dlm = LocalDLMEmulator.newBuilder()
                                  .zkPort(port)
                                  .serverConf(sc)
                                  .build();
        dlm.start();
        this.dlogProcess.set(dlm);

        // Create a namespace.
        Tool tool = ReflectionUtils.newInstance(DistributedLogAdmin.class.getName(), Tool.class);
        tool.run(new String[]{
                "bind",
                "-l", "/ledgers",
                "-s", String.format("%s:%s", DLOG_HOST, port),
                "-c", String.format("distributedlog://%s:%s/%s", DLOG_HOST, port, DLOG_NAMESPACE)});
    }

    private void stopDistributedLog() throws Exception {
        val dlm = this.dlogProcess.getAndSet(null);
        if (dlm != null) {
            dlm.teardown();
        }
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
            val factory = new DistributedLogDataLogFactory(CLIENT_ID + "_secondary", config.get(), executorService());
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
