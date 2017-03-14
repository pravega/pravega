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
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import com.twitter.distributedlog.tools.Tool;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for DistributedLogDataLog. These require that a compiled DistributedLog distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class DistributedLogDataLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final String DLOG_HOST = "127.0.0.1";
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
        DLOG_PORT.set(TestUtils.randomPort());
        String classPath = getClassPath();
        val pb = new ProcessBuilder(
                "java",
                "-cp", String.join(":", classPath),
                DistributedLogStarter.class.getCanonicalName(),
                DLOG_HOST,
                Integer.toString(DLOG_PORT.get()));
        pb.inheritIO(); // For some reason we need to inherit IO, otherwise this process is not responsive and we can't use it.
        DLOG_PROCESS.set(pb.start());
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
        Tool tool = ReflectionUtils.newInstance(DistributedLogAdmin.class.getName(), Tool.class);
        tool.run(new String[]{
                "bind",
                "-l", "/ledgers",
                "-s", String.format("%s:%s", DLOG_HOST, DLOG_PORT.get()),
                "-c", String.format("distributedlog://%s:%s/%s", DLOG_HOST, DLOG_PORT.get(), namespace)});

        // Setup config to use the port and namespace.
        this.config.set(new TestConfig()
                .withDistributedLogHost(DLOG_HOST)
                .withDistributedLogPort(DLOG_PORT.get())
                .withDistributedLogNamespace(namespace));

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

    /**
     * Gets the current class path and updates the path to Guava to point to version 16.0 of it.
     */
    private static String getClassPath() {
        String[] classPath = System.getProperty("java.class.path").split(":");
        String guava16Path = getGuava16PathFromSystemProperties();
        if (guava16Path == null) {
            guava16Path = inferGuava16PathFromClassPath(classPath);
        }

        Assert.assertTrue("Unable to determine Guava 16 path.", guava16Path != null && guava16Path.length() > 0);
        for (int i = 0; i < classPath.length; i++) {
            if (classPath[i].contains("guava")) {
                classPath[i] = guava16Path;
            }
        }
        return String.join(":", classPath);
    }

    private static String getGuava16PathFromSystemProperties() {
        String guava16Path = System.getProperty("user.guava16");
        if (guava16Path != null && guava16Path.length() > 2 && guava16Path.startsWith("[") && guava16Path.endsWith("]")) {
            guava16Path = guava16Path.substring(1, guava16Path.length() - 1);
        }

        return guava16Path;
    }

    @SneakyThrows(IOException.class)
    private static String inferGuava16PathFromClassPath(String[] classPath) {
        // Example path: /home/username/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/16.0/aca09d2e5e8416bf91550e72281958e35460be52/guava-16.0.jar
        final String searchString = "com.google.guava/guava";
        final Path jarPath = Paths.get("guava-16.0.jar");

        for (String path : classPath) {
            if (path.contains("guava")) {
                int dirNameStartPos = path.indexOf(searchString);
                if (dirNameStartPos >= 0) {
                    String dirName = path.substring(0, dirNameStartPos + searchString.length());
                    Path f = Files.find(Paths.get(dirName), 3, (p, a) -> p.endsWith(jarPath))
                                  .findFirst().orElse(null);
                    if (f != null) {
                        return f.toString();
                    }
                }
            }
        }

        return null;
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
        try (DurableDataLog log = createDurableDataLog()) {
            log.initialize(TIMEOUT);

            // Simulate a different client trying to open the same log. This should not be allowed.
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
