/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Helper class for iterating over list of chunks.
 */
class ChunkIterator {
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final MetadataTransaction txn;
    private volatile String currentChunkName;
    private volatile String lastChunkName;
    private volatile ChunkMetadata currentMetadata;

    ChunkIterator(ChunkedSegmentStorage chunkedSegmentStorage, MetadataTransaction txn, SegmentMetadata segmentMetadata) {
        this.chunkedSegmentStorage = Preconditions.checkNotNull(chunkedSegmentStorage, "chunkedSegmentStorage");
        this.txn = Preconditions.checkNotNull(txn, "txn");
        Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        // The following can be null.
        this.currentChunkName = segmentMetadata.getFirstChunk();
    }

    ChunkIterator(ChunkedSegmentStorage chunkedSegmentStorage, MetadataTransaction txn, String startChunkName, String lastChunkName) {
        this.chunkedSegmentStorage = Preconditions.checkNotNull(chunkedSegmentStorage, "chunkedSegmentStorage");
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
                        }, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor());
    }
}
