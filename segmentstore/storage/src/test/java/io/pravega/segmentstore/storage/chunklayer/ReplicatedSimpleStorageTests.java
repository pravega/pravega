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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ReplicatedChunkStorage} using {@link SimpleStorageTests}.
 */
public class ReplicatedSimpleStorageTests extends SimpleStorageTests {
    @Override
    protected ChunkStorage getChunkStorage() {
        return getReplicatedChunkStorage(executorService());
    }

    private static ChunkStorage getReplicatedChunkStorage(ScheduledExecutorService executorService) {
        // These are set up so that
        // Every 2nd read call fails on chunkStorage1
        // Every 4th read call fails on both chunkStorage1, chunkStorage2
        // chunkStorage3 never fails.
        // This way at least one chunkstorage always succeeds.
        // In other words, even with 0, 1 or 2 failures the overall call always succeeds.
        val chunkStorage1 = new FlakyChunkStorage(new InMemoryChunkStorage(executorService), executorService);
        chunkStorage1.getInterceptor().getFlakyPredicates().add(FlakinessPredicate.builder()
                        .method("doRead.before")
                        .matchPredicate(n -> n % 2 == 0)  // every alternate read fails (even numbered).
                        .matchRegEx("") // match any chunk
                        .action(() -> {
                            throw new ChunkNotFoundException("chunk", "intentional");
                        })
                        .build());

        // Every 2nd call comes to this instance, and every 4th fails.
        val chunkStorage2 = new FlakyChunkStorage(new InMemoryChunkStorage(executorService), executorService);
        chunkStorage2.getInterceptor().getFlakyPredicates().add(FlakinessPredicate.builder()
                .method("doRead.before")
                .matchPredicate(n -> n % 2 == 0)  // every alternate read fails (even numbered).
                .matchRegEx("") // match any chunk
                .action(() -> {
                    throw new ChunkNotFoundException("chunk", "intentional");
                })
                .build());

        // Always succeeds.
        val chunkStorage3 = new FlakyChunkStorage(new InMemoryChunkStorage(executorService), executorService);

        val storages = new AsyncBaseChunkStorage[]{ chunkStorage1, chunkStorage2, chunkStorage3};
        return new ReplicatedChunkStorage(storages, executorService);
    }

    /**
     * Unit tests for {@link ReplicatedChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class ReplicatedChunkStorageRollingStorageTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return getReplicatedChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link ReplicatedChunkStorage} using {@link ChunkStorageTests}.
     */
    public static class ReplicatedChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return getReplicatedChunkStorage(executorService());
        }

        @Test
        public void testCapabilities() {
            assertEquals(false, chunkStorage.supportsAppend());
            assertEquals(false, chunkStorage.supportsTruncation());
            assertEquals(false, chunkStorage.supportsConcat());
        }
    }

    public static class ReplicatedChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return getReplicatedChunkStorage(executorService());
        }
    }
}
