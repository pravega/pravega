package io.pravega.client.tables;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TableReader<K,V> extends AutoCloseable {
    /**
     * Gets the latest value for the given key, including its latest KeyVersion.
     */
    CompletableFuture<GetResult> get(K key);

    /**
     * Multi-gets the latest value for the given keys.
     */
    CompletableFuture<List<GetResult<V>>> get(List<K> keys);

    /**
     * Registers a listener for all updates to one or more keys.
     */
    void registerListener(KeyUpdateListener<K, V> listener);

    /**
     * Unregisters a registered listener.
     */
    void unregisterListener(KeyUpdateListener<K, V> listener);
}
