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

package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * InMemoryMetadataStore stores the key-values in memory.
 */
public class InMemoryMetadataStore extends BaseMetadataStore {
    /**
     * Optional callback to invoke  during {@link BaseMetadataStore#read(String)} call.
     */
    @Getter
    @Setter
    Function<TransactionData, CompletableFuture<TransactionData>> readCallback;

    /**
     * Optional callback to invoke during {@link BaseMetadataStore#writeAll(Collection)} call.
     */
    @Getter
    @Setter
    Function<Collection<TransactionData>, CompletableFuture<TransactionData>> writeCallback;

    private final AtomicBoolean entryTracker = new AtomicBoolean(false);

    /**
     * Backing in memory data.
     */
    @Getter
    private final Map<String, TransactionData> backingStore = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param config Configuration options for this instance.
     * @param executor Executor to use.
     */
    public InMemoryMetadataStore(ChunkedSegmentStorageConfig config, Executor executor) {
        super(config, executor);
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return Associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     */
    @Override
    protected CompletableFuture<TransactionData> read(String key) {
        synchronized (this) {
            TransactionData data = backingStore.get(key);
            if (null == data) {
                data = TransactionData.builder()
                        .key(key)
                        .persisted(true)
                        .dbObject(this)
                        .build();
            }

            val retValue = data;
            CompletableFuture<TransactionData> future = CompletableFuture.completedFuture(retValue);

            if (readCallback != null) {
                future = future.thenComposeAsync(v ->
                    readCallback.apply(retValue)
                        .thenApplyAsync(vv -> retValue, getExecutor()),
                getExecutor());
            }

            return future;
        }
    }

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     */
    @Override
    protected CompletableFuture<Void> writeAll(Collection<TransactionData> dataList) {
        CompletableFuture<TransactionData> f;
        if (writeCallback != null) {
            f = writeCallback.apply(dataList);
        } else {
            f = CompletableFuture.completedFuture(null);
        }
        return f.thenRunAsync(() -> {
            synchronized (this) {
                Preconditions.checkState(!entryTracker.getAndSet(true), "writeAll should never be called concurrently");
                try {
                    for (TransactionData data : dataList) {
                        Preconditions.checkState(null != data.getKey());
                        val key = data.getKey();
                        val oldValue = backingStore.get(key);
                        if (oldValue != null) {
                            Preconditions.checkState(this == oldValue.getDbObject(), "Data is not owned.");
                            if (!(oldValue.getVersion() < data.getVersion())) {
                                Preconditions.checkState(oldValue.getVersion() <= data.getVersion(), "Attempt to overwrite newer version");
                            }
                        }
                        data.setDbObject(this);
                        Preconditions.checkState(!data.isPinned(), "Pinned data should not be stored");
                        backingStore.put(key, data);
                    }
                } finally {
                    entryTracker.set(false);
                }
            }
        }, getExecutor());
    }

    /**
     * Retrieve all key-value pairs stored in this instance of {@link ChunkMetadataStore}.
     * There is no order guarantee provided.
     *
     * @return A CompletableFuture that, when completed, will contain {@link Stream} of {@link StorageMetadata} entries.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    @Override
    public CompletableFuture<Stream<StorageMetadata>> getAllEntries() {
        return CompletableFuture.completedFuture(backingStore.values().stream()
                .filter(transactionData -> transactionData != null && transactionData.getValue() != null)
                .map(TransactionData::getValue));
    }

    /**
     * Retrieve all keys stored in this instance of {@link ChunkMetadataStore}.
     * There is no order guarantee provided.
     *
     * @return A CompletableFuture that, when completed, will contain {@link Stream} of  {@link String} keys.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    @Override
    public CompletableFuture<Stream<String>> getAllKeys() {
        return CompletableFuture.completedFuture(backingStore.values().stream()
                .filter(transactionData -> transactionData != null && transactionData.getValue() != null)
                .map(TransactionData::getKey));
    }

    /**
     * Creates a clone for given {@link InMemoryMetadataStore}. Useful to simulate zombie segment container.
     *
     * @param original Metadata store to clone.
     * @return Clone of given instance.
     */
    public static InMemoryMetadataStore clone(InMemoryMetadataStore original) {
        InMemoryMetadataStore cloneStore = new InMemoryMetadataStore(original.getConfig(), original.getExecutor());
        synchronized (original) {
            synchronized (cloneStore) {
                cloneStore.setVersion(original.getVersion());
                for (val entry : original.backingStore.entrySet()) {
                    val key = entry.getKey();
                    val transactionData = entry.getValue();
                    val cloneData = transactionData.toBuilder()
                            .key(key)
                            .value(transactionData.getValue() != null ? transactionData.getValue().deepCopy() : null)
                            .version(transactionData.getVersion())
                            .build();
                    Preconditions.checkState(transactionData.equals(cloneData));
                    // Make sure it is a deep copy.
                    if (transactionData.getValue() != null) {
                        Preconditions.checkState(transactionData.getValue() != cloneData.getValue());
                    }
                    cloneData.setDbObject(cloneStore);
                    cloneStore.backingStore.put(key, cloneData);
                }
            }
        }
        return cloneStore;
    }
}

