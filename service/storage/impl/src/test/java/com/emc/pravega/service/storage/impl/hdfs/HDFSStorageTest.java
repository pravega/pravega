/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.ExceptionHelpers;
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
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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

    @Test // TODO: delete this.
    public void testOngoing() {
        String segmentName = "TestSegment";

        AtomicInteger epoch = new AtomicInteger(0);
        AtomicLong offset = new AtomicLong(0);
        AtomicInteger writeId = new AtomicInteger(0);
        Supplier<byte[]> nextWriteData = () -> ("Write_" + Integer.toString(writeId.incrementAndGet())).getBytes();

        // Epoch: 1
        try (val storage = createStorage()) {
            storage.initialize(epoch.incrementAndGet());
            System.err.println("Exists (pre-create): " + storage.exists(segmentName, TIMEOUT).join());
            System.err.println("Create: " + storage.create(segmentName, TIMEOUT).join());
            System.err.println("Exists (post-create): " + storage.exists(segmentName, TIMEOUT).join());
            System.err.println("GetInfo (post-create): " + storage.getStreamSegmentInfo(segmentName, TIMEOUT).join());

            val writeHandle1 = storage.openWrite(segmentName).join();
            System.err.println("WriteHandle (epoch 1): " + writeHandle1);
        }

        // Epoch: 3
        try (val storage = createStorage()) {
            storage.initialize(epoch.incrementAndGet());
            try {
                storage.create(segmentName, TIMEOUT).join();
                Assert.fail();
            } catch (Throwable ex) {
                ex = ExceptionHelpers.getRealException(ex);
                System.err.println("Create (epoch 2): " + ex.getClass().getSimpleName() + " " + ex.getMessage());
            }

            System.err.println("GetInfo (epoch 2): " + storage.getStreamSegmentInfo(segmentName, TIMEOUT).join());

            val writeHandle = storage.openWrite(segmentName).join();
            System.err.println("WriteHandle (epoch 2): " + writeHandle);

            // Do some writes.
            for (int i = 0; i < 10; i++) {
                byte[] data = nextWriteData.get();
                System.err.println("Write at offset " + offset.get() + ": " + new String(data));
                storage.write(writeHandle, offset.get(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();
                offset.addAndGet(data.length);
            }
        }

        // Epoch: 3
        try (val storage = createStorage()) {
            storage.initialize(epoch.incrementAndGet());
            System.err.println("GetInfo (epoch 3): " + storage.getStreamSegmentInfo(segmentName, TIMEOUT).join());

            val writeHandle = storage.openWrite(segmentName).join();
            System.err.println("WriteHandle (epoch 3): " + writeHandle);
            System.err.println("GetInfo (post-open-write): " + storage.getStreamSegmentInfo(segmentName, TIMEOUT).join());

            val readHandle1 = storage.openRead(segmentName).join();
            System.err.println("ReadHandle (post-open-read): " + readHandle1);

            // Do some writes.
            for (int i = 0; i < 10; i++) {
                byte[] data = nextWriteData.get();
                System.err.println("Write at offset " + offset.get() + ": " + new String(data));
                storage.write(writeHandle, offset.get(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();
                offset.addAndGet(data.length);
            }


            // Do some reading.
            byte[] readBuffer = new byte[(int)offset.get()];
            int readBytes = storage.read(readHandle1,0,readBuffer,0,readBuffer.length, TIMEOUT).join();
            System.err.println("ReadHandle (post-read): " + readHandle1);
            System.err.println(String.format("Read Result (%d bytes): %s", readBytes, new String(readBuffer, 0, readBytes)));

            storage.seal(writeHandle, TIMEOUT).join();
            System.err.println("GetInfo4 (post-seal): " + storage.getStreamSegmentInfo(segmentName, TIMEOUT).join());
            System.err.println("Seal.WriteHandle (post-seal): " + writeHandle);
            System.err.println("New.WriteHandle (post-seal): " + storage.openWrite(segmentName).join());

            try {
                storage.write(writeHandle, offset.get(), new ByteArrayInputStream("fail".getBytes()), 4, TIMEOUT).join();
                Assert.fail("No exception caught.");
            } catch (Throwable ex) {
                ex = ExceptionHelpers.getRealException(ex);
                System.err.println("Write (epoch 2, post-seal): " + ex.getClass().getSimpleName() + " " + ex.getMessage());
            }

            storage.delete(writeHandle, TIMEOUT).join();
            System.err.println("Exists (post-delete): " + storage.exists(segmentName, TIMEOUT).join());

            try {
                storage.write(writeHandle, offset.get(), new ByteArrayInputStream("fail".getBytes()), 4, TIMEOUT).join();
                Assert.fail();
            } catch (Throwable ex) {
                ex = ExceptionHelpers.getRealException(ex);
                System.err.println("Write (epoch 3, post-delete): " + ex.getClass().getSimpleName() + " " + ex.getMessage());
            }
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
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should only be able to execute Read-Only operations.
            SegmentHandle handle2 = createHandle(segmentName, false, epoch2);
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
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        //return new HDFSSegmentHandle(segmentName, readOnly, epoch);
        return null; //TODO: fix
    }

    @Override
    protected Storage createStorage() {
        // Create a config object, using all defaults, except for the HDFS URL.
        HDFSStorageConfig config = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                .build();
        return new HDFSStorage(config, executorService());
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
            return super.getStreamSegmentInfo(handle.getSegmentName(), Duration.ofSeconds(10)).join().isSealed();
        }
    }
}
