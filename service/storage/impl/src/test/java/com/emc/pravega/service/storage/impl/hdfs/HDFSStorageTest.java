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
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageTestBase;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.emc.pravega.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
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

    //region Fencing tests

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Test
    @Override
    public void testFencing() throws Exception {
        final String segmentName = "segment";
        try (val storage1 = createStorage(0);
             val storage2 = createStorage(1)) {

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            verifyWriteOperationsSucceed(segmentName, storage1);
            verifyReadOnlyOperationsSucceed(segmentName, storage1);

            // Storage2 should only be able to execute Read-Only operations.
            verifyWriteOperationsFail(segmentName, storage2);
            verifyReadOnlyOperationsSucceed(segmentName, storage2);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            storage2.open(segmentName).join();

            // Storage1 should be able to execute only read-only operations.
            verifyWriteOperationsFail(segmentName, storage1);
            verifyReadOnlyOperationsSucceed(segmentName, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(segmentName, storage2);
            verifyWriteOperationsSucceed(segmentName, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsFail(segmentName, storage1);
            verifyFinalWriteOperationsSucceed(segmentName, storage2);
        }
    }

    private void verifyReadOnlyOperationsSucceed(String segmentName, Storage storage) {
        boolean exists = storage.exists(segmentName, TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(segmentName, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    private void verifyWriteOperationsSucceed(String segmentName, Storage storage) {
        val si = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        storage.write(segmentName, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        storage.write(concatName, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatName, TIMEOUT).join();
        storage.concat(segmentName, si.getLength() + data.length, concatName, TIMEOUT).join();
    }

    private void verifyWriteOperationsFail(String segmentName, Storage storage) {
        val si = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertThrows(
                "Write was not fenced out.",
                () -> storage.write(segmentName, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        storage.write(concatName, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatName, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Concat was not fenced out.",
                () -> storage.concat(segmentName, si.getLength() + data.length, concatName, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatName, TIMEOUT).join();
    }

    private void verifyFinalWriteOperationsSucceed(String segmentName, Storage storage) {
        storage.seal(segmentName, TIMEOUT).join();
        storage.delete(segmentName, TIMEOUT).join();

        boolean exists = storage.exists(segmentName, TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    private void verifyFinalWriteOperationsFail(String segmentName, Storage storage) {
        AssertExtensions.assertThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(segmentName, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(segmentName, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(segmentName, TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    //endregion

    @Override
    @SneakyThrows(IOException.class)
    protected Storage createStorage() {
        return createStorage(0);
    }

    private Storage createStorage(int pravegaId) throws IOException {
        // Create a config object, using all defaults, except for the HDFS URL.
        val config = new TestConfig()
                .withPravegaId(pravegaId)
                .withHdfsHostURL(String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()));
        val storage = new MiniClusterPermFixer(config, executorService());
        storage.initialize();
        return storage;
    }

    /**
     * Wrapper for a storage class which handles the ACL behavior of MiniDFSCluster.
     * This keeps track of the sealed segments and throws error when a write is attempted on a segment.
     **/
    private static class MiniClusterPermFixer extends HDFSStorage {
        MiniClusterPermFixer(HDFSStorageConfig config, Executor executor) {
            super(config, executor);
        }

        @Override
        public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
            if (isSealed(streamSegmentName)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(streamSegmentName));
            }

            return super.write(streamSegmentName, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset,
                                              String sourceStreamSegmentName, Duration timeout) {
            if (isSealed(targetStreamSegmentName)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(targetStreamSegmentName));
            }

            return super.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
        }

        private boolean isSealed(String streamSegmentName) {
            // It turns out MiniHDFSCluster does not respect file attributes when it comes to writing: a R--R--R-- file
            // will gladly be modified without throwing any sort of exception, so here we are, trying to "simulate" this.
            // It should be noted though that a regular HDFS installation works just fine, which is why this code should
            // not make it in the HDFSStorage class itself.
            try {
                return super.getStreamSegmentInfoSync(streamSegmentName).isSealed();
            } catch (IOException ex) {
                return false;
            }
        }
    }

    private static class TestConfig extends HDFSStorageConfig {
        private String hdfsHostURL;
        private int pravegaId;

        TestConfig() throws ConfigurationException {
            super(PropertyBag.create());
        }

        @Override
        public String getHDFSHostURL() {
            return this.hdfsHostURL;
        }

        @Override
        public int getPravegaID() {
            return this.pravegaId;
        }

        TestConfig withHdfsHostURL(String value) {
            this.hdfsHostURL = value;
            return this;
        }

        TestConfig withPravegaId(int value) {
            this.pravegaId = value;
            return this;
        }
    }
}
