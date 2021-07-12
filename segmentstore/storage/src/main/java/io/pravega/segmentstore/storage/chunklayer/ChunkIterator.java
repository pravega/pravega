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

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * Helper class for iterating over list of chunks.
 */
class ChunkIterator {
    private final Executor executor;
    private final MetadataTransaction txn;
    private volatile String currentChunkName;
    private volatile String lastChunkName;
    private volatile ChunkMetadata currentMetadata;

    ChunkIterator(Executor executor, MetadataTransaction txn, SegmentMetadata segmentMetadata) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.txn = Preconditions.checkNotNull(txn, "txn");
        Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        // The following can be null.
        this.currentChunkName = segmentMetadata.getFirstChunk();
    }

    ChunkIterator(Executor executor, MetadataTransaction txn, String startChunkName, String lastChunkName) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.txn = Preconditions.checkNotNull(txn, "txn");
        // The following can be null.
        this.currentChunkName = startChunkName;
        this.lastChunkName = lastChunkName;
    }

    public CompletableFuture<Void> forEach(BiConsumer<ChunkMetadata, String> consumer) {
        return Futures.loop(
                () -> null != currentChunkName && !currentChunkName.equals(lastChunkName),
                () -> txn.get(currentChunkName)
                        .thenAcceptAsync(storageMetadata -> {
                            currentMetadata = (ChunkMetadata) storageMetadata;
                            consumer.accept(currentMetadata, currentChunkName);
                            // Move next
                            currentChunkName = currentMetadata.getNextChunk();
                        }, executor),
                executor);
    }
}
