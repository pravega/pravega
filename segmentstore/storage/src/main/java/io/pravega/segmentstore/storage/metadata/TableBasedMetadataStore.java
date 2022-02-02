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
package io.pravega.segmentstore.storage.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_FOUND_IN_STORE;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_NOT_FOUND;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.TABLE_GET_LATENCY;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.TABLE_WRITE_LATENCY;

/**
 * {@link TableStore} based storage metadata store.
 */
@Slf4j
public class TableBasedMetadataStore extends BaseMetadataStore {
    private final static BaseMetadataStore.TransactionData.TransactionDataSerializer SERIALIZER = new BaseMetadataStore.TransactionData.TransactionDataSerializer();

    /**
     * Instance of the {@link TableStore}.
     */
    @Getter
    private final TableStore tableStore;

    /**
     * Name of the table segment.
     */
    @Getter
    private final String tableName;
    private final Duration timeout = Duration.ofSeconds(30);
    private final AtomicBoolean isTableInitialized = new AtomicBoolean(false);

    /**
     * Constructor.
     * @param tableName  Name of the table segment.
     * @param tableStore Instance of the {@link TableStore}.
     * @param config Configuration options for this instance.
     * @param executor Executor to use for async operations.
     */
    public TableBasedMetadataStore(String tableName, TableStore tableStore, ChunkedSegmentStorageConfig config, Executor executor) {
        super(config, executor);
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return Associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     */
    @Override
    protected CompletableFuture<TransactionData> read(String key) {
        val keys = new ArrayList<BufferView>();
        keys.add(new ByteArraySegment(key.getBytes(Charsets.UTF_8)));
        val t = new Timer();
        return ensureInitialized()
                .thenComposeAsync(v -> this.tableStore.get(tableName, keys, timeout)
                        .thenApplyAsync(entries -> {
                            try {
                                Preconditions.checkState(entries.size() == 1, "Unexpected number of values returned.");
                                val entry = entries.get(0);
                                if (null != entry) {
                                    val arr = entry.getValue();
                                    TransactionData txnData = SERIALIZER.deserialize(arr);
                                    txnData.setDbObject(entry.getKey().getVersion());
                                    txnData.setPersisted(true);
                                    TABLE_GET_LATENCY.reportSuccessEvent(t.getElapsed());
                                    METADATA_FOUND_IN_STORE.inc();
                                    return txnData;
                                }
                            } catch (Exception e) {
                                throw new CompletionException(new StorageMetadataException("Error while reading", e));
                            }
                            TABLE_GET_LATENCY.reportSuccessEvent(t.getElapsed());
                            METADATA_NOT_FOUND.inc();
                            return TransactionData.builder()
                                    .key(key)
                                    .persisted(true)
                                    .dbObject(TableKey.NOT_EXISTS)
                                    .build();
                        }, getExecutor())
                        .exceptionally(e -> {
                                val ex = Exceptions.unwrap(e);
                                throw new CompletionException(handleException(ex));
                        }), getExecutor());
    }

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     */
    @Override
    protected CompletableFuture<Void> writeAll(Collection<TransactionData> dataList) {
        val toUpdate = new ArrayList<TableEntry>();
        val entryToTxnDataMap = new HashMap<TableEntry, TransactionData>();
        val deletedKeyToTxnDataMap = new HashMap<TableKey, TransactionData>();
        val keysToDelete = new ArrayList<TableKey>();
        val t = new Timer();
        return ensureInitialized()
                .thenRunAsync(() -> {
                    for (TransactionData txnData : dataList) {
                        Preconditions.checkState(null != txnData.getDbObject(), "Missing tracking object");

                        val version = (Long) txnData.getDbObject();
                        if (null == txnData.getValue()) {
                            val toDelete = TableKey.unversioned(new ByteArraySegment(txnData.getKey().getBytes(Charsets.UTF_8)));
                            keysToDelete.add(toDelete);
                            deletedKeyToTxnDataMap.put(toDelete, txnData);
                        }

                        try {
                            val arraySegment = SERIALIZER.serialize(txnData);
                            TableEntry tableEntry = TableEntry.versioned(
                                    new ByteArraySegment(txnData.getKey().getBytes(Charsets.UTF_8)),
                                    arraySegment,
                                    version);
                            entryToTxnDataMap.put(tableEntry, txnData);
                            toUpdate.add(tableEntry);
                        } catch (Exception e) {
                            throw new CompletionException(handleException(e));
                        }
                    }
                }, getExecutor())
                .thenComposeAsync(v -> {
                    // Now put uploaded keys.
                    // toUpdate includes both modified keys as well updates to deleted keys to mark them as deleted.
                    return this.tableStore.put(tableName, toUpdate, timeout)
                            .thenComposeAsync(ret -> {
                                // Update versions.
                                int i = 0;
                                for (TableEntry tableEntry : toUpdate) {
                                    entryToTxnDataMap.get(tableEntry).setDbObject(ret.get(i));
                                    i++;
                                }
                                // Delete deleted keys. They were already "marked as deleted" in earlier step.
                                // This next step will just remove them from table store.
                                return this.tableStore.remove(tableName, keysToDelete, timeout)
                                        .handleAsync((v1, ex) -> {
                                            // Ignore any exception. WriteAll should succeed irrespective of whether removal
                                            // of keys marked for deletion succeeds or fails. This is because all the changed
                                            // keys are already persisted successfully in earlier step.
                                            if (ex == null) {
                                                deletedKeyToTxnDataMap.values().stream().forEach(txnData -> txnData.setDbObject(TableKey.NOT_EXISTS));
                                            } else {
                                                log.warn("Error while deleting keys from table segment {}.", tableName, ex);
                                            }
                                            TABLE_WRITE_LATENCY.reportSuccessEvent(t.getElapsed());
                                            return v1;
                                        }, getExecutor());
                            }, getExecutor());
                }, getExecutor())
                .exceptionally(e -> {
                    val ex = Exceptions.unwrap(e);
                    throw new CompletionException(handleException(ex));
                });
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
        return this.tableStore.entryIterator(tableName, IteratorArgs.builder().fetchTimeout(timeout).build())
                .thenApplyAsync(i -> getStreamFromTableIterator(i).map(td -> td.getValue()), getExecutor())
                .exceptionally(e -> {
                    val ex = Exceptions.unwrap(e);
                    throw new CompletionException(handleException(ex));
                });
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
        return this.tableStore.entryIterator(tableName, IteratorArgs.builder().fetchTimeout(timeout).build())
                .thenApplyAsync(i -> getStreamFromTableIterator(i).map(td -> td.getKey()), getExecutor())
                .exceptionally(e -> {
                    val ex = Exceptions.unwrap(e);
                    throw new CompletionException(handleException(ex));
                });
    }

    /**
     * Converts the given {@link AsyncIterator} to {@link Stream} of {@link TransactionData}.
     * @param iterator Iterator to convert.
     * @return {@link Stream} of {@link TransactionData}.
     */
    Stream<TransactionData> getStreamFromTableIterator(AsyncIterator<IteratorItem<TableEntry>> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator.asIterator(), Spliterator.CONCURRENT), true)
                .map(collection -> collection.getEntries())
                .flatMap(entry -> entry.stream())
                .map(tableEntry -> {
                    try {
                        return SERIALIZER.deserialize(tableEntry.getValue());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private StorageMetadataException handleException(Throwable ex) {
        val e = Exceptions.unwrap(ex);
        if (e instanceof DataLogWriterNotPrimaryException) {
            return new StorageMetadataWritesFencedOutException("Transaction failed. Writer fenced off", e);
        }
        if (e instanceof BadKeyVersionException) {
            return new StorageMetadataVersionMismatchException("Transaction failed. Version Mismatch.", e);
        }
        return new StorageMetadataException("Transaction failed", e);
    }

    private CompletableFuture<Void> ensureInitialized() {
        if (!isTableInitialized.get()) {
            // Storage Metadata Segment is a System, Internal Segment. It must also be designated as Critical since the
            // Segment Store may not function properly without it performing well. The Critical designation will cause
            // all of its "modify" operations to bypass any ingestion pipeline throttling and be expedited for processing.
            val segmentType = SegmentType.builder().tableSegment().system().critical().internal().build();
            return this.tableStore.createSegment(tableName, segmentType, timeout)
                    .thenRunAsync(() -> {
                        log.debug("Created table segment {}", tableName);
                        isTableInitialized.set(true);
                    }, getExecutor())
                    .exceptionally(e -> {
                        val ex = Exceptions.unwrap(e);
                        if (e.getCause() instanceof StreamSegmentExistsException) {
                            log.debug("Table segment {} already exists.", tableName);
                            isTableInitialized.set(true);
                            return null;
                        }
                        throw new CompletionException(ex);
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Copy the version of one instance to other.
     * This only for test purposes.
     *
     * @param from The instance to copy from.
     * @param to   The instance to copy to.
     */
    @VisibleForTesting
    static void copyVersion(TableBasedMetadataStore from, TableBasedMetadataStore to) {
        to.setVersion(from.getVersion());
    }
}
