package io.pravega.client.tables;

import java.util.concurrent.CompletableFuture;

public interface TableTransaction<K, V> extends AutoCloseable {
    // Same as TableWriter.put()
    CompletableFuture<KeyVersion> put(K key, V value, KeyVersion compareVersion);

    // Same as TableWriter.remove()
    CompletableFuture<KeyVersion> remove(K key, KeyVersion compareVersion);

    /**
     * Commits the contents of this Transaction into the parent Table as one
     * atomic update.
     */
    CompletableFuture<Void> commit();

    /**
     * Aborts this transaction.
     */
    CompletableFuture<Void> abort();
}
