/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.storage.DataCorruptionException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
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
import static org.mockito.Mockito.*;

/**
 * Unit tests for HDFSStorage that use mocks.
 */
public class HDFSStorageTestWithMocks {
    private static final String SEG_SEALED_PATH = "/seg_sealed";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final int HIGHER_EPOCH = 11;
    private static final int MIDDLE_EPOCH = 10;
    private static final String MIDDLE_EPOCH_PATH = "/seg_10";
    private static final String HIGHER_EPOCH_PATH = "/seg_11";
    private static final String LOWER_EPOCH_PATH = "/seg_9";
    private static final String ZERO_EPOCH_PATH = "/seg_0";
    private static final String TEST_SEGMENT_NAME = "seg";
    private static final String DUMMY_MESSAGE = "dummy exception message";

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
                        new FileStatus(0, false, 1, 64, 0, new Path(MIDDLE_EPOCH_PATH)),
                        new FileStatus(0, false, 1, 64, 0, new Path(LOWER_EPOCH_PATH)),
                        new FileStatus(0, false, 1, 64, 0, new Path(ZERO_EPOCH_PATH)),
                    }
                );
        when(mockFileSystem.create(any(), eq(READWRITE_PERMISSION), anyBoolean(), anyInt(), anyShort(), anyLong(), any()))
                .thenReturn(new FSDataOutputStream(new ByteArrayOutputStream()));

        when(mockFileSystem.rename(any(), any()))
                .thenReturn(true);

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        storage.create(TEST_SEGMENT_NAME);
        verify(mockFileSystem).rename(new Path(MIDDLE_EPOCH_PATH), new Path(HIGHER_EPOCH_PATH));
        verify(mockFileSystem).delete(new Path(LOWER_EPOCH_PATH), true);
        verify(mockFileSystem).delete(new Path(ZERO_EPOCH_PATH), true);
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
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                        new FileStatus(0, false, 1, 64, 0, new Path(ZERO_EPOCH_PATH)),
                }
        );
        when(mockFileSystem.create(any(), eq(READWRITE_PERMISSION), anyBoolean(), anyInt(), anyShort(), anyLong(), any()))
                .thenReturn( new FSDataOutputStream(new ByteArrayOutputStream()));

        when(mockFileSystem.rename(any(), any()))
                .thenReturn(true);

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);

        AssertExtensions.assertThrows("Create should throw StorageNotPrimaryException when file  with higher epoch exists",
                () -> storage.create(TEST_SEGMENT_NAME),
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
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows("create should throw StreamSegmentExistsException",
                () -> storage.create(TEST_SEGMENT_NAME),
                ex -> ex instanceof StreamSegmentExistsException);
    }

    @Test
    public void testOpenWriteWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenThrow(new IOException(DUMMY_MESSAGE));
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows("openWrite should correctly throw IOException.",
                () -> storage.openWrite(TEST_SEGMENT_NAME),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testOpenReadWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenThrow(new IOException(DUMMY_MESSAGE));
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows("openRead should correctly throw IOException.",
                () -> storage.openRead(TEST_SEGMENT_NAME),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testCreateWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenThrow(new IOException(DUMMY_MESSAGE));
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows("create should correctly throw IOException.",
                () -> storage.create(TEST_SEGMENT_NAME),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testExistsWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenThrow(new IOException(DUMMY_MESSAGE));
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        // should not throw Exception
        storage.exists(TEST_SEGMENT_NAME);
    }

    @Test
    public void testgetStreamSegmentInfoWithException() throws Exception {
        when(mockFileSystem.globStatus(any())).thenThrow(new IOException(DUMMY_MESSAGE));
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows(" should correctly throw IOException.",
                () -> storage.getStreamSegmentInfo(TEST_SEGMENT_NAME),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testUnsealWithException() throws Exception {
        when(mockFileSystem.rename(any(), any())).thenThrow(new IOException(DUMMY_MESSAGE));
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        AssertExtensions.assertThrows("seal should correctly throw IOException.",
                () -> storage.unseal(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testSealWithException() throws Exception {
        when(mockFileSystem.rename(any(), any())).thenThrow(new IOException(DUMMY_MESSAGE));
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("seal should correctly throw IOException.",
                () -> storage.seal(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testDeleteWithException() throws Exception {
        when(mockFileSystem.delete(any(), anyBoolean())).thenThrow(new IOException(DUMMY_MESSAGE));
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );
        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("delete should correctly throw IOException.",
                () -> storage.delete(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testDeleteForPessimisticCaseWithException() throws Exception {
        when(mockFileSystem.delete(any(), anyBoolean()))
                .thenThrow(new PathNotFoundException(HIGHER_EPOCH_PATH))
                .thenThrow(new IOException(HIGHER_EPOCH_PATH));

        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        AssertExtensions.assertThrows("delete should correctly throw IOException.",
                () -> storage.delete(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    /**
     * Tests scenario when generic IOException is thrown.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testSealForException() throws Exception {
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new IOException(MIDDLE_EPOCH_PATH));
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(MIDDLE_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("seal should correctly handle IOException.",
                () -> storage.seal(handle),
                ex -> ex instanceof IOException);
        verify(mockFileSystem, times(1)).rename(new Path(MIDDLE_EPOCH_PATH), new Path(SEG_SEALED_PATH));
        // Make sure file permissions are not changed.
        verify(mockFileSystem, never()).setPermission(any(), any());
    }

    @Test
    public void testSealForPessimisticCase() throws Exception {
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(HIGHER_EPOCH_PATH))
                .thenReturn(true);
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        storage.seal(handle);
        verify(mockFileSystem, times(2)).rename(new Path(HIGHER_EPOCH_PATH), new Path(SEG_SEALED_PATH));
    }

    /**
     * Tests scenario where file with higher epoch is found in storage after earlier successful openWrite.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testSealForPessimisticCaseWithHigherEpochInStorage() throws Exception {
        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(MIDDLE_EPOCH_PATH))
                .thenReturn(true);

        // First call is during openWrite, second call is during unseal
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(MIDDLE_EPOCH_PATH)),
                }
        ).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("seal should correctly throw StorageNotPrimaryException.",
                () -> storage.seal(handle),
                ex -> ex instanceof StorageNotPrimaryException);
        verify(mockFileSystem, times(1)).rename(new Path(MIDDLE_EPOCH_PATH), new Path(SEG_SEALED_PATH));
    }

    /**
     * Tests scenario where file with lower epoch is found in storage after earlier successful openWrite.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testSealForPessimisticCaseWithLowerEpochInStorage() throws Exception {

        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(MIDDLE_EPOCH_PATH))
                .thenReturn(true);

        // First call is during openWrite, second call is during unseal
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(MIDDLE_EPOCH_PATH)),
                }
        ).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(LOWER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("seal should correctly throw DataCorruptionException.",
                () -> storage.seal(handle),
                ex -> ex instanceof DataCorruptionException);
        verify(mockFileSystem, times(1)).rename(new Path(MIDDLE_EPOCH_PATH), new Path(SEG_SEALED_PATH));
    }

    @Test
    public void testUnsealForPessimisticCase() throws Exception {
        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(SEG_SEALED_PATH))
                .thenReturn(true);

        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(SEG_SEALED_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        storage.unseal(handle);
        verify(mockFileSystem, times(2)).rename(new Path(SEG_SEALED_PATH), new Path(HIGHER_EPOCH_PATH));
    }

    /**
     * Tests scenario when generic IOException is thrown.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testUnsealForException() throws Exception {
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new IOException(MIDDLE_EPOCH_PATH));
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(SEG_SEALED_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("unseal should correctly handle IOException.",
                () -> storage.unseal(handle),
                ex -> ex instanceof IOException);
        verify(mockFileSystem, times(1)).rename(new Path(SEG_SEALED_PATH), new Path(MIDDLE_EPOCH_PATH));
        // Make sure file permissions are changed.
        verify(mockFileSystem, times(1)).setPermission(new Path(SEG_SEALED_PATH), new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));
    }

    /**
     * Tests scenario where file with higher epoch is found in storage after earlier successful openWrite.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testUnsealForPessimisticCaseWithHigherEpochInStorage() throws Exception {
        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(SEG_SEALED_PATH))
                .thenReturn(true);

        // First call is during openWrite, second call is during unseal
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(SEG_SEALED_PATH)),
                }
        ).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("unseal should correctly throw StorageNotPrimaryException.",
                () -> storage.unseal(handle),
                ex -> ex instanceof StorageNotPrimaryException);
        verify(mockFileSystem, times(1)).rename(new Path(SEG_SEALED_PATH), new Path(MIDDLE_EPOCH_PATH));
    }

    /**
     * Tests scenario where file with lower epoch is found in storage after earlier successful openWrite.
     * @throws Exception Assertions and other exceptions.
     */
    @Test
    public void testUnsealForPessimisticCaseWithLowerEpochInStorage() throws Exception {

        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(SEG_SEALED_PATH))
                .thenReturn(true);

        // First call is during openWrite, second call is during unseal
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(SEG_SEALED_PATH)),
                }
        ).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(LOWER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);

        AssertExtensions.assertThrows("unseal should correctly throw DataCorruptionException.",
                () -> storage.unseal(handle),
                ex -> ex instanceof DataCorruptionException);
        verify(mockFileSystem, times(1)).rename(new Path(SEG_SEALED_PATH), new Path(MIDDLE_EPOCH_PATH));
    }

    @Test
    public void testDeleteForPessimisticCase() throws Exception {
        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.delete(any(), anyBoolean()))
                .thenThrow(new PathNotFoundException(HIGHER_EPOCH_PATH))
                .thenReturn(true);

        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        storage.delete(handle);
        verify(mockFileSystem, times(2)).delete(new Path(HIGHER_EPOCH_PATH), true);
    }

    @Test
    public void testSealForPessimisticCaseForException() throws Exception {
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(HIGHER_EPOCH_PATH))
                .thenReturn(true);
        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                }
        ).thenThrow(new IOException(DUMMY_MESSAGE));

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        AssertExtensions.assertThrows("seal should correctly throw IOException.",
                () -> storage.seal(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testUnsealForPessimisticCaseForException() throws Exception {
        // First call to rename throws exception, the second call succeeds.
        when(mockFileSystem.rename(any(), any()))
                .thenThrow(new PathNotFoundException(SEG_SEALED_PATH))
                .thenReturn(true);

        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[] {
                        new FileStatus(0, false, 1, 64, 0, new Path(SEG_SEALED_PATH)),
                }
        ).thenThrow(new IOException(DUMMY_MESSAGE));

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);
        SegmentHandle handle = storage.openWrite(TEST_SEGMENT_NAME);
        AssertExtensions.assertThrows("unseal should correctly throw IOException.",
                () -> storage.unseal(handle),
                ex -> ex instanceof IOException && !(ex instanceof StreamSegmentException));
    }

    @Test
    public void testIllegalArgumentException() throws Exception {

        when(mockFileSystem.globStatus(any())).thenReturn(
                new FileStatus[]{
                        new FileStatus(0, false, 1, 64, 0, new Path(HIGHER_EPOCH_PATH)),
                        new FileStatus(0, false, 1, 64, 0, new Path(MIDDLE_EPOCH_PATH)),
                }
        );

        HDFSStorage storage = createStorage();
        storage.initialize(HIGHER_EPOCH);

        AssertExtensions.assertThrows("Presence of multiple files correctly results in IllegalArgumentException.",
                () -> storage.exists(TEST_SEGMENT_NAME),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFormatException() throws Exception {
        HDFSStorage storage = createStorage();
        storage.initialize(MIDDLE_EPOCH);

        String[] malformedStrings = new String[] {
                "/seg-10",
                "/seg10",
                "/seg-abc",
                "/seg_abc",
                "/seg_1.0",
        };

        for (String malformedString : malformedStrings) {
            when(mockFileSystem.globStatus(any())).thenReturn(
                    new FileStatus[]{
                            new FileStatus(0, false, 1, 64, 0, new Path(malformedString)),
                    }
            );

            AssertExtensions.assertThrows("Malformed files present",
                    () -> storage.openWrite(TEST_SEGMENT_NAME),
                    ex -> ex instanceof IllegalStateException || ex instanceof FileNameFormatException);
        }
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
