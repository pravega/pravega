/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageFullException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import lombok.Cleanup;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileSystemChunkStorage} that uses mocks.
 */
public class FileSystemChunkStorageMockTest extends ThreadPooledTestSuite {
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
    public void testWithNonRegularFile() throws Exception {
        String chunkName = "test";

        FileChannel channel = mock(FileChannel.class);
        fixChannelMock(channel);

        FileSystemWrapper fileSystemWrapper = mock(FileSystemWrapper.class);
        when(fileSystemWrapper.exists(any())).thenReturn(true);
        when(fileSystemWrapper.isRegularFile(any())).thenReturn(false);
        @Cleanup
        FileSystemChunkStorage testStorage = new FileSystemChunkStorage(storageConfig, fileSystemWrapper, executorService());
        AssertExtensions.assertFutureThrows(
                " openRead should throw ChunkStorageException.",
                testStorage.openRead(chunkName),
                ex -> ex instanceof ChunkStorageException && ex.getMessage().contains("chunk is not a regular file"));

        AssertExtensions.assertFutureThrows(
                " openRead should throw ChunkStorageException.",
                testStorage.openWrite(chunkName),
                ex -> ex instanceof ChunkStorageException && ex.getMessage().contains("chunk is not a regular file"));
    }

    @Test
    public void testWithRandomException() throws Exception {
        String chunkName = "test";

        FileSystemWrapper fileSystemWrapper = mock(FileSystemWrapper.class);
        when(fileSystemWrapper.getFileSize(any())).thenThrow(new IOException("Random"));
        when(fileSystemWrapper.getFileChannel(any(), any())).thenThrow(new IOException("Random"));
        when(fileSystemWrapper.setPermissions(any(), any())).thenThrow(new IOException("Random"));
        when(fileSystemWrapper.createDirectories(any())).thenThrow(new IOException("Random"));
        doThrow(new IOException("Random")).when(fileSystemWrapper).delete(any());

        @Cleanup
        FileSystemChunkStorage testStorage = new FileSystemChunkStorage(storageConfig, fileSystemWrapper, executorService());
        AssertExtensions.assertThrows(
                " doDelete should throw ChunkStorageException.",
                () -> testStorage.doDelete(ChunkHandle.writeHandle(chunkName)),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertThrows(
                " doSetReadOnly should throw ChunkStorageException.",
                () -> testStorage.doSetReadOnly(ChunkHandle.writeHandle(chunkName), false),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertThrows(
                " doGetInfo should throw ChunkStorageException.",
                () -> testStorage.doGetInfo(chunkName),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertThrows(
                " create should throw ChunkStorageException.",
                () -> testStorage.doCreate(chunkName),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertFutureThrows(
                " write should throw exception.",
                testStorage.write(ChunkHandle.writeHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertFutureThrows(
                " read should throw exception.",
                testStorage.read(ChunkHandle.writeHandle(chunkName), 0, 1, new byte[1], 0),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

        AssertExtensions.assertFutureThrows(
                " concat should throw exception.",
                testStorage.concat(new ConcatArgument[]{
                                ConcatArgument.builder().name("A").length(0).build(),
                                ConcatArgument.builder().name("B").length(1).build()
                        }),
                ex -> ex instanceof ChunkStorageException
                        && ex.getCause() instanceof IOException && ex.getCause().getMessage().equals("Random"));

    }

    @Test
    public void testStorageFull() throws Exception {
        val fs = spy(FileSystemWrapper.class);
        doThrow(new IOException("No space left on device")).when(fs).getFileSize(any());
        FileSystemChunkStorage storage = new FileSystemChunkStorage(storageConfig, fs, executorService());
        AssertExtensions.assertFutureThrows("should throw ChunkStorageFull exception",
                storage.getInfo("test"),
                ex -> ex instanceof ChunkStorageFullException);
    }

    @Test
    public void testGetUsageException() {
        val fs = spy(FileSystemWrapper.class);
        doThrow(new RuntimeException("Intentional")).when(fs).getUsedSpace(any());
        FileSystemChunkStorage storage = new FileSystemChunkStorage(storageConfig, fs, executorService());
        AssertExtensions.assertFutureThrows("should throw ChunkStorageException exception",
                storage.getUsedSpace(),
                ex -> ex instanceof ChunkStorageException);
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
        String chunkName = "test";

        // Set up mocks.
        FileChannel channel = mock(FileChannel.class);
        fixChannelMock(channel);

        FileSystemWrapper fileSystemWrapper = mock(FileSystemWrapper.class);
        when(fileSystemWrapper.getFileChannel(any(), any())).thenReturn(channel);
        when(fileSystemWrapper.getFileSize(any())).thenReturn(2L * bufferSize);

        @Cleanup
        FileSystemChunkStorage testStorage = new FileSystemChunkStorage(storageConfig, fileSystemWrapper, executorService());

        ChunkHandle handle = ChunkHandle.readHandle(chunkName);

        // Force two reads.
        ArgumentCaptor<Long> expectedArgs = ArgumentCaptor.forClass(Long.class);
        when(channel.read(any(), anyLong())).thenReturn(index, bufferSize - index);

        // Call method.
        byte[] buffer = new byte[bufferSize];
        testStorage.doRead(handle, 0, bufferSize, buffer, 0);

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
}
