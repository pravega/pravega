/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.StorageTestBase;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for HDFSStorage.
 */
public class HDFSStorageTest extends StorageTestBase {
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;

    @Before
    public void setUp() throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.OFF);
        //context.reset();

        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setBoolean("dfs.permissions.enabled", true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
    }

    @After
    public void tearDown() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
        }
    }

    @Test
    @Override
    public void testFencing() throws Exception {

    }

    @Override
    @SneakyThrows(IOException.class)
    protected Storage createStorage() {
        // Create a config object, using all defaults, except for the HDFS URL.
        val config = new TestConfig().withHdfsHostURL(String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()));
        val storage = new MiniClusterPermFixer(config, executorService());
        storage.initialize();
        return storage;
    }

    /**
     * Wrapper for a storage class which handles the ACL behavior of MiniDFSCluster.
     * This keeps track of the sealed segments and throws error when a write is attempted on a segment.
     **/
    private static class MiniClusterPermFixer extends HDFSStorage {
        private final Set<String> sealedList;

        MiniClusterPermFixer(HDFSStorageConfig config, Executor executor) {
            super(config, executor);
            sealedList = Collections.synchronizedSet(new HashSet<>());
        }

        @Override
        public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
            if (sealedList.contains(streamSegmentName)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(streamSegmentName));
            }

            return super.write(streamSegmentName, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
            sealedList.add(streamSegmentName);
            CompletableFuture<SegmentProperties> result = super.seal(streamSegmentName, timeout);
            result.exceptionally(ex -> {
                sealedList.remove(streamSegmentName);
                return null;
            });

            return result;
        }

        @Override
        public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset,
                                              String sourceStreamSegmentName, Duration timeout) {
            if (sealedList.contains(targetStreamSegmentName)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(targetStreamSegmentName));
            }

            return super.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
        }
    }

    private static class TestConfig extends HDFSStorageConfig {
        private String hdfsHostURL;

        TestConfig() throws ConfigurationException {
            super(PropertyBag.create());
        }

        public String getHDFSHostURL() {
            return this.hdfsHostURL;
        }

        TestConfig withHdfsHostURL(String value) {
            this.hdfsHostURL = value;
            return this;
        }
    }
}
