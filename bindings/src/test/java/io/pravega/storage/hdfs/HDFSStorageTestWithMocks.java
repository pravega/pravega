/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HDFSStorage that use mocks.
 */
public class HDFSStorageTestWithMocks {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);

    private HDFSStorageConfig adapterConfig;
    private FileSystem mockFileSystem;

    @Before
    public void setUp() throws Exception {
        mockFileSystem = Mockito.mock(FileSystem.class);
        this.adapterConfig = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", 1234))
                .build();
    }

    @After
    public void tearDown() {
        mockFileSystem = null;
    }

    private HDFSStorage createStorage() {
        return new TestHDFSStorage(mockFileSystem, this.adapterConfig);
    }

    /**
     * Tests the case when create has a race with there are multiple files.
     * Eg.
     *  Storage with epoch 9, 10 and 11 all race. they all find that segment does not exist
     *  Storage 9 creates the file seg_0 and renames it.
     *  Before it can return Storage 10 creates file seg_0 again and renames it.
     *  Before both can return Storage 11 also starts executing and creates seg_0 again.
     *  @throws Exception The test can throw exception.
     */
    @Test
    public void testCreateRaceWithZombieFiles() throws Exception {
        // Two different calls return different data simulating race condition.
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {},
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path("/seg_10")),
                        new FileStatus(0, false, 1, 64, 0, new Path("/seg_9")),
                        new FileStatus(0, false, 1, 64, 0, new Path("/seg_0")),
                    }
                );
        when(mockFileSystem.create(any(), eq(READWRITE_PERMISSION), anyBoolean(), anyInt(), anyShort(), anyLong(), any()))
                .thenReturn(new FSDataOutputStream(new ByteArrayOutputStream()));

        when(mockFileSystem.rename(any(), any()))
                .thenReturn(true);

        HDFSStorage storage = createStorage();
        storage.initialize(11);
        storage.create("seg");
        verify(mockFileSystem).rename(new Path("/seg_10"), new Path("/seg_11"));
        verify(mockFileSystem).delete(new Path("/seg_9"), true);
        verify(mockFileSystem).delete(new Path("/seg_0"), true);
    }

    /**
     * Tests the case when create has a race with there are multiple files.
     * Eg.
     *  Storage with epoch 10 and 11 race. They all find that segment does not exist.
     *  Storage 11 creates the file seg_0 and renames it.
     *  Before it can return Storage 10 creates file seg_0 again and tries to claim it.
     *  @throws Exception The test can throw exception.
     */
    @Test
    public void testCreateRaceWithZombieFilesNotPrimary() throws Exception {
        // Two different calls return different data simulating race condition.
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {},
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path("/seg_11")),
                        new FileStatus(0, false, 1, 64, 0, new Path("/seg_0")),
                }
        );
        when(mockFileSystem.create(any(), eq(READWRITE_PERMISSION), anyBoolean(), anyInt(), anyShort(), anyLong(), any()))
                .thenReturn( new FSDataOutputStream(new ByteArrayOutputStream()));

        when(mockFileSystem.rename(any(), any()))
                .thenReturn(true);

        HDFSStorage storage = createStorage();
        storage.initialize(10);

        AssertExtensions.assertThrows("Create should throw StorageNotPrimaryException when file  with higher epoch exists",
                () -> storage.create("seg"),
                ex -> ex instanceof StorageNotPrimaryException);
    }

    /**
     * Tests the case when create has a race where two segments are trying to create seg_0 file.
     * Eg.
     *  Storage with epoch 10 and 11  race. they all find that segment does not exist
     *  Storage 10 creates the file seg_0. At this point Storage 11 also starts executing and tries to creates seg_0 again.
     *  It should correctly throw StreamSegmentExistsException
     *  @throws Exception The test can throw exception.
     */
    @Test
    public void testCreateRaceWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {}
        );
        when(mockFileSystem.create(any(), eq(READWRITE_PERMISSION), anyBoolean(), anyInt(), anyShort(), anyLong(), any()))
                .thenThrow(new FileAlreadyExistsException());

        HDFSStorage storage = createStorage();
        storage.initialize(11);

        AssertExtensions.assertThrows("create should throw StreamSegmentExistsException",
                () -> storage.create("seg"),
                ex -> ex instanceof StreamSegmentExistsException);

    }



    //region TestHDFSStorage
    /**
     * TestHDFSStorage class that uses Mockito mock for file system.
     **/
    private static class TestHDFSStorage extends HDFSStorage {
        FileSystem mockFileSystem;
        TestHDFSStorage(FileSystem mockFileSystem, HDFSStorageConfig config) {
            super(config);
            this.mockFileSystem = mockFileSystem;
        }

        @Override
        protected FileSystem openFileSystem(Configuration conf) throws IOException {
            return mockFileSystem;
        }
    }
    //endregion
}
