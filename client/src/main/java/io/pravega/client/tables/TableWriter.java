package io.pravega.client.tables;

import java.util.concurrent.CompletableFuture;

public interface TableWriter<KeyT, ValueT> extends AutoCloseable {
    /**
     * Updates the value of the given key. If compareVersion is not null, the update
     * will only succeed if the current Version of the Key matches the one given.
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value, KeyVersion compareVersion);

    /**
     * Removes the given key. If compareVersion is not null, the update will only
     * succeed if the current Version of the Key matches the one given.
     */
    CompletableFuture<Void> remove(KeyT key, KeyVersion compareVersion);

    /**
     * Begins a new Transaction for this Table.
     */
    CompletableFuture<TableTransaction<KeyT, ValueT>> beginTransaction();

    @Override
    void close();
}
