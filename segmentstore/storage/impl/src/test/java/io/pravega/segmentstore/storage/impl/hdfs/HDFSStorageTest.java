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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.test.common.AssertExtensions;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.Executor;

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
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.OFF);

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
    @Test
    public void testMisc() throws Exception {
        @Cleanup
        val s1 = new HDFSStorage(this.adapterConfig, this.executorService());
        s1.initialize(1);
        val dfs1 = (DistributedFileSystem) s1.getContext().fileSystem;

        val s2 = new HDFSStorage(this.adapterConfig, this.executorService());
        s2.initialize(1);
        val dfs2 = (DistributedFileSystem) s2.getContext().fileSystem;

        val p = new Path("/hello/foo");
        dfs1.create(p).close();
        val os1 = dfs1.append(p);
        os1.write("1".getBytes());
        os1.close();
        System.out.println(1);
        dfs1.close();

        for(int i =0;i<100;i++) {
            try {
                val os2 = dfs2.append(p); // This should work since we closed the lease
                System.out.println(2);
                os2.write("2".getBytes());
                os2.close();
            }catch (Exception ex){
                ex.printStackTrace();
                Thread.sleep(100);
                continue;
            }
            break;
        }
        System.out.println(3);
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
