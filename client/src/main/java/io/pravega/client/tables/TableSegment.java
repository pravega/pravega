package io.pravega.client.tables;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.Getter;

public interface TableSegment<KeyT, ValueT> extends AutoCloseable {
    /**
     * Updates the value of the given key. If compareVersion is not null, the update
     * will only succeed if the current Version of the Key matches the one given.
     */
    CompletableFuture<KeyVersion> put(VersionedEntry<KeyT, ValueT> entry);

    CompletableFuture<List<KeyVersion>> put(List<VersionedEntry<KeyT, ValueT>> entries);

    /**
     * Removes the given key. If compareVersion is not null, the update will only
     * succeed if the current Version of the Key matches the one given.
     */
    CompletableFuture<Void> remove(VersionedKey<KeyT> key);

    CompletableFuture<Void> remove(List<VersionedKey<KeyT>> keys);

    @Override
    void close();

    @Data
    class VersionedKey<KeyT> {
        private final KeyT key;
        private final KeyVersion version;
    }

    @Getter
    class VersionedEntry<KeyT, ValueT> extends VersionedKey<KeyT> {
        private final ValueT value;

        VersionedEntry(KeyT key, ValueT value, KeyVersion version) {
            super(key, version);
            this.value = value;
        }
    }
}
