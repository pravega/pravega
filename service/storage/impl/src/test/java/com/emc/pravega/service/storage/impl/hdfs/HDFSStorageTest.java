/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.StorageTestBase;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
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

    @Override
    @SneakyThrows(IOException.class)
    protected Storage createStorage() {
        // Create a config object, using all defaults, except for the HDFS URL.
        // TODO: see if we can reuse ConfigHelpers from Storage Tests here, to avoid this code duplication.
        Properties prop = new Properties();
        prop.setProperty(
                String.format("%s.%s", HDFSStorageConfig.COMPONENT_CODE, HDFSStorageConfig.PROPERTY_HDFS_URL),
                String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()));

        HDFSStorageConfig config = new HDFSStorageConfig(prop);
        val storage = new HDFSStorage(config, executorService());
        storage.initialize();
        return new MiniClusterPermFixer(storage);
    }

    /**
     * Wrapper for a storage class which handles the ACL behavior of MiniDFSCluster.
     * This keeps track of the sealed segments and throws error when a write is attempted on a segment.
     **/
    private static class MiniClusterPermFixer implements Storage {
        private final HDFSStorage storage;
        private ArrayList<String> sealedList;

        public MiniClusterPermFixer(HDFSStorage storage) {
            sealedList = new ArrayList<>();
            this.storage = storage;
        }

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
            return storage.create(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length,
                                             Duration timeout) {

            if (sealedList.contains(streamSegmentName)) {
                CompletableFuture<Void> retVal = new CompletableFuture<>();
                retVal.completeExceptionally(new StreamSegmentSealedException(streamSegmentName));
                return retVal;
            }
            return storage.write(streamSegmentName, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
            addToSealedList(streamSegmentName);
            return storage.seal(streamSegmentName, timeout);
        }

        private void addToSealedList(String streamSegmentName) {
            sealedList.add(streamSegmentName);
        }

        @Override
        public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset,
                                              String sourceStreamSegmentName, Duration timeout) {
            return storage.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
            return storage.delete(streamSegmentName, timeout);
        }

        @Override
        public void close() {
            storage.close();
        }

        @Override
        public CompletableFuture<Void> open(String streamSegmentName) {
            return storage.open(streamSegmentName);
        }

        @Override
        public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer,
                                               int bufferOffset, int length, Duration timeout) {
            return storage.read(streamSegmentName, offset, buffer, bufferOffset, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return storage.getStreamSegmentInfo(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            return storage.exists(streamSegmentName, timeout);
        }
    }
}
