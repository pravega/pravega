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
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FileSystemChunkStorageCacheTests extends ThreadPooledTestSuite {
    private static final int TEST_DATA_LENGTH = 1024 * 1024 + 17;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    @Test
    public void testConcurrentConcatsReuseCachedSourceChannelSafely() {
        FileSystemStorageConfig config = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, temporaryFolder.getRoot().getAbsolutePath())
                .build();
        byte[] sourceData = createTestData();
        byte[] firstPrefix = new byte[] {9, 8, 7};
        byte[] secondPrefix = new byte[] {6, 5};

        try (FileSystemChunkStorage storage = new FileSystemChunkStorage(config, executorService())) {
            create(storage, "source", sourceData);
            create(storage, "target1", firstPrefix);
            create(storage, "target2", secondPrefix);

            CompletableFuture<Integer> firstConcat = storage.concat(
                    concatArguments("target1", firstPrefix.length, "source", sourceData.length));
            CompletableFuture<Integer> secondConcat = storage.concat(
                    concatArguments("target2", secondPrefix.length, "source", sourceData.length));

            assertEquals(sourceData.length, firstConcat.join().intValue());
            assertEquals(sourceData.length, secondConcat.join().intValue());
            assertArrayEquals(concatenate(firstPrefix, sourceData), read(storage, "target1", firstPrefix.length + sourceData.length));
            assertArrayEquals(concatenate(secondPrefix, sourceData), read(storage, "target2", secondPrefix.length + sourceData.length));
        }
    }

    private static ConcatArgument[] concatArguments(String target, int targetLength, String source, int sourceLength) {
        return new ConcatArgument[] {
                ConcatArgument.builder().name(target).length(targetLength).build(),
                ConcatArgument.builder().name(source).length(sourceLength).build()
        };
    }

    private static void create(FileSystemChunkStorage storage, String name, byte[] contents) {
        storage.create(name).join();
        storage.write(ChunkHandle.writeHandle(name), 0, contents.length, new ByteArrayInputStream(contents)).join();
    }

    private static byte[] read(FileSystemChunkStorage storage, String name, int length) {
        byte[] result = new byte[length];
        assertEquals(length, storage.read(ChunkHandle.readHandle(name), 0, length, result, 0).join().intValue());
        return result;
    }

    private static byte[] createTestData() {
        byte[] result = new byte[TEST_DATA_LENGTH];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (i % 251);
        }
        return result;
    }

    private static byte[] concatenate(byte[] first, byte[] second) {
        byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
