/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for HDFSStorage.
 */
public class HDFSStorageTest extends StorageTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private HDFSStorageConfig adapterConfig;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
        this.adapterConfig = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                .build();
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
     * A special case Fencing test that verifies the HDFSStorage fencing mechanism when the "fenced-out" instance keeps
     * trying to write continuously. This verifies that any ongoing writes are properly handled upon fencing.
     * By having the "fenced-out" instance keep writing, we will exercise the case when we have an ongoing write while
     * a HDFS file is being set as read-only (the HDFS behavior in this case is that the ongoing write will complete).
     */
    @Test(timeout = 60000)
    public void testZombieFencing() throws Exception {
        final long epochCount = 30;
        final int writeSize = 1000;
        final String segmentName = "Segment";
        val writtenData = new EnhancedByteArrayOutputStream();
        final Random rnd = new Random(0);
        int currentEpoch = 1;

        // Create initial adapter.
        val currentStorage = new AtomicReference<Storage>();
        currentStorage.set(createStorage());
        currentStorage.get().initialize(currentEpoch);

        // Create the Segment and open it for the first time.
        val currentHandle = new AtomicReference<SegmentHandle>(
                currentStorage.get().create(segmentName, TIMEOUT)
                        .thenCompose(v -> currentStorage.get().openWrite(segmentName))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        // Run a number of epochs.
        while (currentEpoch <= epochCount) {
            val oldStorage = currentStorage.get();
            val handle = currentHandle.get();
            val writeBuffer = new byte[writeSize];
            val appends = Futures.loop(
                    () -> true,
                    () -> {
                        rnd.nextBytes(writeBuffer);
                        return oldStorage.write(handle, writtenData.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT)
                                .thenRun(() -> writtenData.write(writeBuffer));
                    },
                    executorService());

            // Create a new Storage adapter with a new epoch and open-write the Segment, remembering its handle.
            val newStorage = createStorage();
            try {
                newStorage.initialize(++currentEpoch);
                currentHandle.set(newStorage.openWrite(segmentName).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            } catch (Exception ex) {
                newStorage.close();
                throw ex;
            }

            currentStorage.set(newStorage);
            try {
                appends.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.fail("Continuous appends on older epoch Adapter did not fail.");
            } catch (Exception ex) {
                val cause = Exceptions.unwrap(ex);
                if (!(cause instanceof StorageNotPrimaryException || cause instanceof StreamSegmentSealedException)) {
                    // We only expect the appends to fail because they were fenced out or the Segment was sealed.
                    Assert.fail("Unexpected exception " + cause);
                }
            } finally {
                oldStorage.close();
            }
        }

        byte[] expectedData = writtenData.toByteArray();
        byte[] readData = new byte[expectedData.length];
        @Cleanup
        val readStorage = createStorage();
        readStorage.initialize(++currentEpoch);
        int bytesRead = readStorage
                .openRead(segmentName)
                .thenCompose(handle -> readStorage.read(handle, 0, readData, 0, readData.length, TIMEOUT))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of bytes read.", readData.length, bytesRead);
        Assert.assertArrayEquals("Unexpected data read back.", expectedData, readData);
    }

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

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

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

    //endregion

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        val allFiles = Collections.singletonList(new FileDescriptor(new Path("/" + segmentName + "_0_0"), 0, 0, 0, false));
        if (readOnly) {
            return HDFSSegmentHandle.read(segmentName, allFiles);
        } else {
            return HDFSSegmentHandle.write(segmentName, allFiles);
        }
    }

    @Override
    protected Storage createStorage() {
        return new TestHDFSStorage(this.adapterConfig, executorService());
    }

    //region TestHDFSStorage

    /**
     * Special HDFSStorage that uses a modified version of the MiniHDFSCluster DistributedFileSystem which fixes the
     * 'read-only' permission issues observed with that one.
     **/
    private static class TestHDFSStorage extends HDFSStorage {
        TestHDFSStorage(HDFSStorageConfig config, Executor executor) {
            super(config, executor);
        }

        @Override
        protected FileSystem openFileSystem(Configuration conf) throws IOException {
            return new FileSystemFixer(conf);
        }
    }

    private static class FileSystemFixer extends DistributedFileSystem {
        @SneakyThrows(IOException.class)
        FileSystemFixer(Configuration conf) {
            initialize(FileSystem.getDefaultUri(conf), conf);
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
            if (getFileStatus(f).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(f.getName());
            }

            return super.append(f, bufferSize, progress);
        }

        @Override
        public void concat(Path targetPath, Path[] sourcePaths) throws IOException {
            if (getFileStatus(targetPath).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(targetPath.getName());
            }

            super.concat(targetPath, sourcePaths);
        }

        @Override
        public boolean rename(final Path source, final Path target) throws IOException {
            throw new UnsupportedOperationException("Rename operation disallowed.");
        }
    }

    //endregion
}
