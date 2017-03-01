/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogTestBase;
import com.emc.pravega.service.storage.LogAddress;
import com.twitter.distributedlog.DLSN;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for DistributedLogDataLog. These require that a compiled DistributedLog distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class DistributedLogDataLogTests extends DurableDataLogTestBase {
    private static final int DLOG_PORT = 7000;
    private static final String DLOG_PATH = "/home/andrei/src/distributedlog";
    private static final String DLOG_NAMESPACE = "pravegatest";
    private static final String DLOG_START = DLOG_PATH + "/distributedlog-service/bin/dlog local " + DLOG_PORT;
    private static final String DLOG_CREATE_NAMESPACE = DLOG_PATH + String.format(
            "/distributedlog-service/bin/dlog admin bind -l /ledgers -s 127.0.0.1:%s -c distributedlog://127.0.0.1:%s/%s",
            DLOG_PORT, DLOG_PORT, DLOG_NAMESPACE);

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT_WRITES = 250;
    private static final int WRITE_COUNT_READS = 25;

    private static final DistributedLogConfig CONFIG = new TestConfig()
            .withDistributedLogHost("127.0.0.1")
            .withDistributedLogPort(DLOG_PORT)
            .withDistributedLogNamespace(DLOG_NAMESPACE);

    private final AtomicReference<Process> DLOG_PROCESS = new AtomicReference<>();
    private final AtomicReference<DistributedLogDataLogFactory> FACTORY = new AtomicReference<>();

    @Before
    public void initializeTest() throws Exception {
        System.out.println("DLOG: Starting.");
        DLOG_PROCESS.set(Runtime.getRuntime().exec(DLOG_START));
        System.out.println("DLOG: Started.");

        System.out.println("DLOG: Creating namespace.");
        Process p = Runtime.getRuntime().exec(DLOG_CREATE_NAMESPACE);
        System.out.println("DLOG: Waiting for namespace.");
        int retVal = p.waitFor();
        System.out.println("DLOG: Namespace created. RetVal = " + retVal);

        val factory = new DistributedLogDataLogFactory("UnitTest", CONFIG);
        factory.initialize();
        FACTORY.set(factory);
        System.out.println("DLOG: Factory created.");
    }

    @After
    public void cleanupAfterTest() {
        FACTORY.getAndSet(null).close();
        System.out.println("DLOG: Factory closed.");

        System.out.println("DLOG: Stopping.");
        DLOG_PROCESS.get().destroyForcibly();
        System.out.println("DLOG: Stopped.");
    }

    @Override
    protected DurableDataLog createDurableDataLog() {
        return FACTORY.get().createDurableDataLog(CONTAINER_ID);
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

    }

    private static class TestConfig extends DistributedLogConfig {
        @Getter
        private String distributedLogHost;
        @Getter
        private int distributedLogPort;
        @Getter
        private String distributedLogNamespace;

        public TestConfig() throws ConfigurationException {
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
}
