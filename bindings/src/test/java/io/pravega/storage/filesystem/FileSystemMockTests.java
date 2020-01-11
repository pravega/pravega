/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.SegmentHandle;
import lombok.Getter;
import lombok.Setter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class FileSystemMockTests {
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    private File baseDir = null;
    private FileSystemStorageConfig storageConfig;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        this.storageConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
    }

    @Test
    public void doReadTest() throws Exception {
        doReadTest(0, 1);

        for (int bufferSize : new int[] {2, 3, 4, 1024}) {
            for (int i : new int[] {0, 1, bufferSize / 2, bufferSize - 2, bufferSize -1}) {
                doReadTest(i, bufferSize);
            }
        }
    }

    private void doReadTest(int index, int bufferSize) throws Exception {
        // Set up mocks.
        FileChannel channel = mock(FileChannel.class);
        fixChannelMock(channel);
        String segmentName = "test";

        TestFileSystemStorage testStorage = new TestFileSystemStorage(storageConfig, channel);
        testStorage.setSizeToReturn(2 * bufferSize);
        SegmentHandle handle = FileSystemSegmentHandle.readHandle(segmentName);

        // Force two reads.
        ArgumentCaptor<Long> expectedArgs = ArgumentCaptor.forClass(Long.class);
        when(channel.read(any(), anyLong())).thenReturn(index, bufferSize - index);

        // Call method.
        byte[] buffer = new byte[bufferSize];
        testStorage.read(handle, 0, buffer, 0, bufferSize);

        // Verify.
        verify(channel, times(2)).read(any(), expectedArgs.capture());
        List<Long> actualArgs = expectedArgs.getAllValues();
        assertEquals(2, actualArgs.size());
        assertEquals(0, actualArgs.get(0).longValue());
        assertEquals(index, actualArgs.get(1).longValue());
    }

    private static void fixChannelMock(AbstractInterruptibleChannel mockFileChannel) throws Exception {
        // Note : This is a workaround for NullPointerException.
        // This will break when jdk decides to change implementation.
        Field closeLockField = AbstractInterruptibleChannel.class.getDeclaredField("closeLock");
        closeLockField.setAccessible(true);
        closeLockField.set(mockFileChannel, new Object());
    }

    /**
     * Test Class.
     */
    class TestFileSystemStorage extends FileSystemStorage {
        FileChannel channel;

        @Getter
        @Setter
        long sizeToReturn;

        public TestFileSystemStorage(FileSystemStorageConfig config, FileChannel channel) {
            super(config);
            this.channel = channel;
        }

        @Override
        protected FileChannel getFileChannel(Path path, StandardOpenOption openOption) throws IOException {
            return channel;
        }

        @Override
        protected long getFileSize(Path path) throws IOException {
            return sizeToReturn;
        }
    }
}