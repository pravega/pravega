/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.emc.pravega.service.storage.StorageTestBase;
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
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should only be able to execute Read-Only operations.
            SegmentHandle handle2 = createHandle(segmentName, false);
            verifyWriteOperationsFail(handle2, storage2);
            verifyReadOnlyOperationsSucceed(handle2, storage2);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute only read-only operations.
            verifyWriteOperationsFail(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsFail(handle1, storage1);
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    private void verifyReadOnlyOperationsSucceed(SegmentHandle handle, Storage storage) {
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    private void verifyWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        storage.concat(handle, si.getLength() + data.length, concatHandle, TIMEOUT).join();
    }

    private void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength() + data.length, concatHandle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    private void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    private void verifyFinalWriteOperationsFail(SegmentHandle handle, Storage storage) {
        AssertExtensions.assertThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    //endregion

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly) {
        return new HDFSSegmentHandle(segmentName, readOnly);
    }

    @Override
    @SneakyThrows(IOException.class)
    protected Storage createStorage() {
        return createStorage(0);
    }

    private Storage createStorage(int pravegaId) throws IOException {
        // Create a config object, using all defaults, except for the HDFS URL.
        HDFSStorageConfig config = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.PRAVEGA_ID, pravegaId)
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                .build();
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
        public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
            if (isSealed(handle)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(handle.getSegmentName()));
            }

            return super.write(handle, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, SegmentHandle sourceHandle, Duration timeout) {
            if (isSealed(targetHandle)) {
                return FutureHelpers.failedFuture(new StreamSegmentSealedException(targetHandle.getSegmentName()));
            }

            return super.concat(targetHandle, offset, sourceHandle, timeout);
        }

        private boolean isSealed(SegmentHandle handle) {
            // It turns out MiniHDFSCluster does not respect file attributes when it comes to writing: a R--R--R-- file
            // will gladly be modified without throwing any sort of exception, so here we are, trying to "simulate" this.
            // It should be noted though that a regular HDFS installation works just fine, which is why this code should
            // not make it in the HDFSStorage class itself.
            try {
                return super.getStreamSegmentInfoSync(handle.getSegmentName()).isSealed();
            } catch (IOException ex) {
                return false;
            }
        }
    }
}
