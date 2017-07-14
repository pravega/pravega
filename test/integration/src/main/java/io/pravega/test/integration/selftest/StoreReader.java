package io.pravega.test.integration.selftest;

import java.util.concurrent.CompletableFuture;

/**
 * Defines a general Reader that can be used to access data
 */
interface StoreReader {
    // Future: indicates when shut down/failed.
    // tailReadHandler: invoked on every tail read.
    CompletableFuture<Void> tailRead(String target, java.util.function.Consumer<ReadItem> tailReadHandler);

    CompletableFuture<ReadItem> readDirect(String target, Object readPointer);

    CompletableFuture<ReadItem> readStorage(String target, Object readPointer);

    interface ReadItem {
        Event getEvent();

        Object getReadPointer(); // Offset or EventPointer
    }
}
