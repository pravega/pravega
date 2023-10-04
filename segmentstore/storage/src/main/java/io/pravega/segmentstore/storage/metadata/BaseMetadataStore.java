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

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ConcurrentHashMultiset;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.COMMIT_LATENCY;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.GET_LATENCY;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_BUFFER_EVICTED_COUNT;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_FOUND_IN_BUFFER;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_FOUND_IN_CACHE;
import static io.pravega.segmentstore.storage.metadata.StorageMetadataMetrics.METADATA_FOUND_IN_TXN;
import static io.pravega.shared.MetricsNames.STORAGE_METADATA_BUFFER_SIZE;
import static io.pravega.shared.MetricsNames.STORAGE_METADATA_CACHE_MISS_RATE;
import static io.pravega.shared.MetricsNames.STORAGE_METADATA_CACHE_SIZE;

/**
 * Implements base metadata store that provides core functionality of metadata store by encapsulating underlying key value store.
 * Derived classes override {@link BaseMetadataStore#read(String)} and {@link BaseMetadataStore#writeAll(Collection)} to write to underlying storage.
 * The minimum requirement for underlying key-value store is to provide optimistic concurrency ( Eg. using versions numbers or etags.)
 *
 *
 * Within a segment store instance there should be only one instance that exclusively writes to the underlying key value store.
 * For distributed systems the single writer pattern must be enforced through external means. (Eg. for table segment based implementation
 * DurableLog's native fencing is used to establish ownership and single writer pattern.)
 *
 * This implementation provides following features that simplify metadata management.
 *
 * All access to and modifications to the metadata the {@link ChunkMetadataStore} must be done through a transaction.
 *
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction(boolean, String...)}
 *
 * Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload of{@link MetadataTransaction#commit()}.
 * Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.
 * Transactions are atomic - either all changes in the transaction are committed or none at all.
 * In addition, Transactions provide snapshot isolation which means that transaction fails 
 * if any of the metadata records read during the transactions are changed outside the transaction after they were read.
 *
 * Within a transaction you can perform following actions on per record basis.
 * <ul>
 * <li>{@link MetadataTransaction#get(String)} Retrieves metadata using for given key.</li>
 * <li>{@link MetadataTransaction#create(StorageMetadata)} Creates a new record.</li>
 * <li>{@link MetadataTransaction#delete(String)} Deletes records for given key.</li>
 * <li>{@link MetadataTransaction#update(StorageMetadata)} Updates the transaction local copy of the record.
 * For each record modified inside the transaction update must be called to mark the record as dirty.</li>
 * </ul>
 * <pre>
 *  // Start a transaction.
 * try (MetadataTransaction txn = metadataStore.beginTransaction()) {
 *      // Retrieve the data from transaction
 *      SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
 *
 *      // Modify retrieved record
 *      // seal if it is not already sealed.
 *      segmentMetadata.setSealed(true);
 *
 *      // put it back transaction
 *      txn.update(segmentMetadata);
 *
 *      // Commit
 *      txn.commit();
 *  } catch (StorageMetadataException ex) {
 *      // Handle Exceptions
 *  }
 *  </pre>
 *
 * Underlying implementation might buffer frequently or recently updated metadata keys to optimize read/write performance.
 * To further optimize it may provide "lazy committing" of changes where there is application specific way to recover from failures.(Eg. when only length of chunk is changed.)
 * In this case {@link MetadataTransaction#commit(boolean)} can be called.Note that otherwise for each commit the data is written to underlying key-value store.
 *
 * There are two special methods provided to handle metadata about data segments for the underlying key-value store. They are useful in avoiding circular references.
 * <ul>
 * <li>A record marked as pinned by calling {@link MetadataTransaction#markPinned(StorageMetadata)} is never written to underlying storage.</li>
 * <li>In addition transaction can be committed using {@link MetadataTransaction#commit(boolean, boolean)} to skip validation step that reads any recently evicted changes from underlying storage.</li>
 * </ul>
 */
@Slf4j
@Beta
abstract public class BaseMetadataStore implements ChunkMetadataStore {
    /**
     * Percentage of cache evicted at any time. (1 / CACHE_EVICTION_RATIO) entries are evicted at once.
     */
    private static final int CACHE_EVICTION_RATIO = 10;

    /**
     * Indicates whether this instance is fenced or not.
     */
    private final AtomicBoolean fenced;

    /**
     * Monotonically increasing number. Keeps track of versions independent of external persistence or transaction mechanism.
     */
    private final AtomicLong version;

    /**
     * Buffer for reading and writing transaction data entries to underlying KV store.
     * This allows lazy storing and avoiding unnecessary load for recently/frequently updated key value pairs.
     * Note that entries in this buffer should not be evicted while transaction using them are in flight.
     */
    private final ConcurrentHashMap<String, TransactionData> bufferedTxnData;

    private final ConcurrentHashMap<Long, MetadataTransaction> activeTxns;

    /**
     * Set of active records from commits that are in-flight. These records should not be evicted until the active commits finish.
     */
    private final ConcurrentHashMultiset<String> activeKeys;

    /**
     * Set of keys from commits that are actively being processed. No concurrent commits on the same keys are allowed to proceed.
     */
    @GuardedBy("lockedKeys")
    private final HashSet<String> lockedKeys = new HashSet<>();

    /**
     * Cache for reading and writing transaction data entries to underlying KV store.
     */
    private final Cache<String, TransactionData> cache;

    /**
     * Storage executor object.
     */
    @Getter(AccessLevel.PROTECTED)
    private final Executor executor;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    @Setter
    private int maxEntriesInTxnBuffer;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    @Setter
    private int maxEntriesInCache;

    /**
     * Keep count of records in buffer. ConcurrentHashMap.size() is an expensive operation.
     */
    private final AtomicInteger bufferCount = new AtomicInteger(0);

    /**
     * Flag to keep track of whether the eviction is currently running.
     */
    private final AtomicBoolean isEvictionRunning = new AtomicBoolean();

    /**
     * Lock object to synchronize on during eviction.
     */
    private final Object evictionLock = new Object();

    /**
     * Configuration options for this instance.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    /**
     * Constructs a BaseMetadataStore object.
     *
     * @param config Configuration options for this instance.
     * @param executor Executor to use for async operations.
     */
    public BaseMetadataStore(ChunkedSegmentStorageConfig config, Executor executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        version = new AtomicLong(System.currentTimeMillis()); // Start with unique number.
        fenced = new AtomicBoolean(false);
        bufferedTxnData = new ConcurrentHashMap<>(); // Don't think we need anything fancy here. But we'll measure and see.
        activeTxns = new ConcurrentHashMap<>();
        activeKeys = ConcurrentHashMultiset.create();
        maxEntriesInTxnBuffer = config.getMaxEntriesInTxnBuffer();
        maxEntriesInCache = config.getMaxEntriesInCache();
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxEntriesInCache)
                .build();
    }

    /**
     * Begins a new transaction.
     *
     * @param keysToLock Array of keys to lock for this transaction.
     * @return Returns a new instance of MetadataTransaction.
     */
    @Override
    public MetadataTransaction beginTransaction(boolean isReadonly, String... keysToLock) {
        // Each transaction gets a unique number which is monotonically increasing.
        val txn = new MetadataTransaction(this, isReadonly, version.incrementAndGet(), keysToLock);
        activeTxns.put(txn.getVersion(), txn);
        return txn;
    }

    /**
     * Closes the transaction.
     * @param txn transaction to close.
     */
    @Override
    public void closeTransaction(MetadataTransaction txn) {
        activeTxns.remove(txn.getVersion());
    }

    @Override
    public boolean isTransactionActive(long txnId) {
        return activeTxns.containsKey(txnId);
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * {@link StorageMetadataException} if transaction can not be committed.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn, boolean lazyWrite) {
        return commit(txn, lazyWrite, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn transaction to commit.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * {@link StorageMetadataException} if transaction can not be committed.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn) {
        return commit(txn, false, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * {@link StorageMetadataException} if transaction can not be committed.
     */
    @Override
    public CompletableFuture<Void> commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) {
        Preconditions.checkArgument(null != txn, "txn must not be null");
        Preconditions.checkState(!txn.isReadonly(), "Attempt to modify in readonly transaction");

        val txnData = txn.getData();

        val modifiedKeys = new ArrayList<String>();
        val modifiedValues = new ArrayList<TransactionData>();
        val t = new Timer();
        val shouldReleaseKeys = new AtomicBoolean(false);
        val retValue = CompletableFuture.runAsync(() -> {
                    if (fenced.get()) {
                        throw new CompletionException(new StorageMetadataWritesFencedOutException(
                                String.format("Transaction writer is fenced off. transaction=%s", txn.getVersion())));
                    }
                }, executor)
                .thenComposeAsync(v -> {
                    // Mark keys in transaction as active to prevent their eviction.
                    txn.getData().keySet().forEach(this::addToActiveKeySet);
                    // Prevent any concurrent transactions on keys.
                    acquireKeys(txn);
                    shouldReleaseKeys.set(true);
                    // Step 1 : If bufferedTxnData data was flushed, then read it back from external source and re-insert in bufferedTxnData buffer.
                    return loadMissingKeys(txn, skipStoreCheck, txnData)
                            .thenComposeAsync(v1 -> {
                                // This check needs to be atomic, with absolutely no possibility of re-entry
                                return performCommit(txn, lazyWrite, txnData, modifiedKeys, modifiedValues);
                            }, executor);
                }, executor)
                .thenRunAsync(() -> {
                    //  Step 5 : Mark transaction as commited.
                    txn.setCommitted();
                }, executor)
                .whenCompleteAsync((v, ex) -> {
                    if (shouldReleaseKeys.get()) {
                        // Release keys.
                        releaseKeys(txn);
                    }
                    // Remove keys from active set.
                    txn.getData().keySet().forEach(this::removeFromActiveKeySet);
                    if (txn.isCommitted()) {
                        txnData.clear();
                    }
                    COMMIT_LATENCY.reportSuccessEvent(t.getElapsed());
                }, executor);

        // Trigger evict
        retValue.thenAcceptAsync(v4 -> {
            //  Step 6 : evict if required.
            evictIfNeeded();
        }, executor);

        return retValue;
    }

    /**
     * Loads missing keys.
     */
    private CompletableFuture<Void> loadMissingKeys(MetadataTransaction txn, boolean skipStoreCheck, Map<String, TransactionData> txnData) {
        val loadFutures = new ArrayList<CompletableFuture<TransactionData>>();
        for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
            Preconditions.checkState(activeKeys.contains(entry.getKey()), "key must be marked active. key=%s", entry.getKey());
            val key = entry.getKey();
            if (skipStoreCheck || entry.getValue().isPinned()) {
                log.trace("Skipping loading key from the store key = {}", key);
            } else {
                // This check is safe to be outside the lock
                val dataFromBuffer = bufferedTxnData.get(key);
                if (null == dataFromBuffer) {
                    loadFutures.add(loadFromStore(key));
                }
            }
        }
        return Futures.allOf(loadFutures)
                .thenRunAsync(() -> {
                    // validate everything is alright.
                    for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
                        val dataFromBuffer = bufferedTxnData.get(entry.getKey());
                        if (!(entry.getValue().isPinned())) {
                            Preconditions.checkState(activeKeys.contains(entry.getKey()), "key must be marked active. key=%s", entry.getKey());
                            Preconditions.checkState(null != dataFromBuffer, "Data from buffer must not be null.");
                            if (!dataFromBuffer.isPinned()) {
                                Preconditions.checkState(null != dataFromBuffer.getDbObject(), "Missing tracking object");
                            }
                        }
                    }
                }, executor);
    }

    /**
     * Performs commit.
     */
    private CompletableFuture<Void> performCommit(MetadataTransaction txn, boolean lazyWrite,
                                                  Map<String, TransactionData> txnData, ArrayList<String> modifiedKeys,
                                                  ArrayList<TransactionData> modifiedValues) {
        return CompletableFuture.runAsync(() -> {
                    // Step 2 : Check whether transaction is safe to commit.
                    validateCommit(txn, txnData, modifiedKeys, modifiedValues);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 3: Commit externally.
                    // This operation may call external storage.
                    return writeToMetadataStore(lazyWrite, modifiedValues);
                }, executor)
                .thenComposeAsync(v -> executeExternalCommitAction(txn), executor)
                .thenRunAsync(() -> {
                    // If we reach here then it means transaction is safe to commit.
                    // Step 4: Update buffer.
                    val committedVersion = version.incrementAndGet();
                    val toAdd = new HashMap<String, TransactionData>();
                    int delta = 0;
                    for (String key : modifiedKeys) {
                        TransactionData data = getDeepCopy(txnData.get(key));
                        data.setVersion(committedVersion);
                        toAdd.put(key, data);
                        if (data.isCreated()) {
                            delta++;
                        }
                        if (data.isDeleted()) {
                            delta--;
                        }
                    }
                    bufferedTxnData.putAll(toAdd);
                    bufferCount.addAndGet(delta);
                }, executor);
    }

    /**
     * Writes modified values to the metadata store.
     */
    private CompletableFuture<Void> writeToMetadataStore(boolean lazyWrite, ArrayList<TransactionData> modifiedValues) {
        if (!lazyWrite || (bufferCount.get() > maxEntriesInTxnBuffer)) {
            log.trace("Persisting all modified keys (except pinned)");
            val toWriteList = modifiedValues.stream().filter(entry -> !entry.isPinned()).collect(Collectors.toList());
            if (toWriteList.size() > 0) {
                return writeAll(toWriteList)
                        .thenRunAsync(() -> {
                            log.trace("Done persisting all modified keys");
                            for (val writtenData : toWriteList) {
                                // Mark written keys as persisted.
                                writtenData.setPersisted(true);
                                // Put it in cache.
                                cache.put(writtenData.getKey(), writtenData);
                            }
                        }, executor);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Executes external commit step.
     */
    private CompletableFuture<Void> executeExternalCommitAction(MetadataTransaction txn) {
        // Execute external commit step.
        try {
            if (null != txn.getExternalCommitStep()) {
                return txn.getExternalCommitStep().call();
            }
        } catch (Exception e) {
            log.error("Exception during execution of external commit step", e);
            throw new CompletionException(new StorageMetadataException("Exception during execution of external commit step", e));
        }
        return CompletableFuture.completedFuture(null);
    }

    private void validateCommit(MetadataTransaction txn, Map<String, TransactionData> txnData, ArrayList<String> modifiedKeys, ArrayList<TransactionData> modifiedValues) {
        for (val entry : txnData.entrySet()) {
            val key = entry.getKey();
            val transactionData = entry.getValue();
            Preconditions.checkState(null != transactionData.getKey(), "Missing key.");

            // See if this entry was modified in this transaction.
            if (transactionData.getVersion() == txn.getVersion()) {
                modifiedKeys.add(key);
                transactionData.setPersisted(false);
                modifiedValues.add(transactionData);
            }
            // make sure none of the keys used in this transaction have changed.
            val dataFromBuffer = bufferedTxnData.get(key);
            if (null != dataFromBuffer) {
                if (!dataFromBuffer.isPinned()) {
                    Preconditions.checkState(null != dataFromBuffer.getDbObject(), "Missing tracking object");
                }
                if (dataFromBuffer.getVersion() > transactionData.getVersion()) {
                    throw new CompletionException(new StorageMetadataVersionMismatchException(
                            String.format("Transaction uses stale data. Key version changed key=%s committed=%s transaction=%s",
                                    key, dataFromBuffer.getVersion(), txnData.get(key).getVersion())));
                }

                // Pin it if it is already pinned.
                transactionData.setPinned(transactionData.isPinned() || dataFromBuffer.isPinned());

                // Set the database object.
                transactionData.setDbObject(dataFromBuffer.getDbObject());
            } else {
                Preconditions.checkState(entry.getValue().isPinned(), "Transaction data evicted unexpectedly. Key=%s", entry.getKey());
            }
        }
    }

    /**
     * Evict entries if needed.
     * Only evict keys that are persisted, not pinned or active.
     */
    private void evictIfNeeded() {
        if (isEvictionRunning.compareAndSet(false, true)) {
            try {
                val limit = 1 + maxEntriesInTxnBuffer / CACHE_EVICTION_RATIO;
                if (bufferCount.get() > maxEntriesInTxnBuffer) {
                    val toEvict = bufferedTxnData.entrySet().parallelStream()
                            .filter(entry -> entry.getValue().isPersisted() && !entry.getValue().isPinned()
                                    && !activeKeys.contains(entry.getKey()))
                            .map(Map.Entry::getKey)
                            .limit(limit)
                            .collect(Collectors.toList());
                    evictFromBuffer(toEvict);
                }
            } finally {
                isEvictionRunning.set(false);
            }
        }
    }

    /**
     * Evict all entries from cache.
     */
    public void evictFromCache() {
        cache.invalidateAll();
    }

    /**
     * Evict all the keys that are eligible.
     */
    public void evictAllEligibleEntriesFromBuffer() {
        try {
            isEvictionRunning.set(true);
            val toEvict = bufferedTxnData.entrySet().parallelStream()
                    .filter(entry -> entry.getValue().isPersisted() && !entry.getValue().isPinned()
                            && !activeKeys.contains(entry.getKey()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            evictFromBuffer(toEvict);
        } finally {
            isEvictionRunning.set(false);
        }
    }

    /**
     * Evict given list of keys.
     * @param keysToEvict
     */
    private void evictFromBuffer(List<String> keysToEvict) {
        int count = 0;
        for (val key : keysToEvict) {
            // synchronize so that we don't accidentally delete a key that becomes active after check here.
            synchronized (evictionLock) {
                if (0 == activeKeys.count(key)) {
                    // Synchronization prevents error when key becomes active between the check and remove.
                    // Move the key to cache
                    val v = bufferedTxnData.get(key);
                    if (null != v) {
                        cache.put(key, v);
                    }
                    // Remove from buffer.
                    bufferedTxnData.remove(key);
                    count++;
                }
            }
        }
        bufferCount.addAndGet(-1 * count);
        METADATA_BUFFER_EVICTED_COUNT.add(count);
        log.debug("{} entries evicted from transaction buffer.", count);
    }

    /**
     * Aborts given transaction.
     *
     * @param txn transaction to abort.
     *            throws StorageMetadataException If there are any errors.
     */
    @Override
    public CompletableFuture<Void> abort(MetadataTransaction txn) {
        Preconditions.checkArgument(null != txn, "txn must not be null");
        // Do nothing
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Retrieves the metadata for given key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @return A CompletableFuture that, when completed, will contain metadata for given key. Null if key was not found.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                             {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    @Override
    public CompletableFuture<StorageMetadata> get(MetadataTransaction txn, String key) {
        Preconditions.checkArgument(null != txn, "txn must not be null");
        if (null == key) {
            return CompletableFuture.completedFuture(null);
        }
        val t = new Timer();
        val txnData = txn.getData();

        // Record is found in transaction data itself.
        TransactionData data = txnData.get(key);
        if (null != data) {
            GET_LATENCY.reportSuccessEvent(t.getElapsed());
            METADATA_FOUND_IN_TXN.inc();
            return CompletableFuture.completedFuture(data.getValue());
        }

        // Prevent the key from getting evicted.
        addToActiveKeySet(key);

        return CompletableFuture.supplyAsync(() -> bufferedTxnData.get(key), executor)
                .thenApplyAsync(dataFromBuffer -> {
                    if (dataFromBuffer != null) {
                        METADATA_FOUND_IN_BUFFER.inc();
                        // Make sure it is a deep copy.
                        return copyToTransaction(txn, key, dataFromBuffer);
                    }
                    return null;
                }, executor)
                .thenComposeAsync(retValue -> {
                    if (retValue != null) {
                        return CompletableFuture.completedFuture(retValue);
                    }
                    // We did not find it in the buffer either.
                    // Try to find it in store.
                    return loadFromStore(key)
                            .thenApplyAsync(dataFromStore -> copyToTransaction(txn, key, dataFromStore), executor);
                }, executor)
                .whenCompleteAsync((v, ex) -> {
                    removeFromActiveKeySet(key);
                    GET_LATENCY.reportSuccessEvent(t.getElapsed());
                }, executor);
    }

    private StorageMetadata copyToTransaction(MetadataTransaction txn, String key, TransactionData transactionData) {
        final TransactionData txnLocalCopy = getDeepCopy(transactionData);
        txn.getData().put(key, txnLocalCopy);
        return txnLocalCopy.getValue();
    }

    private TransactionData getDeepCopy(TransactionData transactionData) {
        val copy = transactionData.toBuilder().build();
        if (null != transactionData.getValue()) {
            // Make sure a deep copy is returned.
            copy.setValue(transactionData.getValue().deepCopy());
        }
        return copy;
    }

    private void removeFromActiveKeySet(String key) {
        // No need to synchronize as activeKeys is already a ConcurrentHashMultiset.
        // In case of any race with eviction logic, the key will simply be evicted next iteration.
        // This is not incorrect and the race should be rare.
        activeKeys.remove(key);
    }

    private void addToActiveKeySet(String key) {
        // No need to synchronize if the eviction is not running as activeKeys is ConcurrentHashMultiset
        if (isEvictionRunning.get()) {
            // However this is required when eviction is happening in background because eviction code checks the count
            // and should evict key only if the count is zero.
            // These two steps are not atomic hence the use of synchronized in this narrow case to prevent race.
            synchronized (evictionLock) {
                activeKeys.add(key);
            }
        } else {
            activeKeys.add(key);
        }
    }

    /**
     * When called marks the keys in the transaction as "locked".
     * Until they are unlocked all further commits on the same keys will fail with VersionMismatch.
     *
     * @param txn Transaction which should get exclusive access to the keys.
     */
    private void acquireKeys(MetadataTransaction txn) {
        synchronized (lockedKeys) {
            for (String key : txn.getKeysToLock()) {
                if (lockedKeys.contains(key)) {
                    throw new CompletionException(new StorageMetadataVersionMismatchException(
                            String.format("Concurrent transaction commits not allowed. key=%s transaction=%s",
                                    key, txn.getVersion())));
                }
            }
            // Now that we have validated, mark all keys as "locked".
            for (String key : txn.getKeysToLock()) {
                lockedKeys.add(key);
            }
        }
    }

    /**
     * When called unmarks the keys in the transaction as "locked".
     *
     * @param txn Transaction which should get exclusive access to the keys.
     */
    private void releaseKeys(MetadataTransaction txn) {
        synchronized (lockedKeys) {
            for (String key : txn.getKeysToLock()) {
                lockedKeys.remove(key);
            }
        }
    }

    /**
     * Loads value from store.
     */
    private CompletableFuture<TransactionData> loadFromStore(String key) {
        log.trace("Loading key from the store key = {}", key);
        return readFromStore(key)
                .thenApplyAsync(copyFromStore -> {
                    Preconditions.checkState(null != copyFromStore, "Data from table store must not be null. key=%s", key);
                    Preconditions.checkState(null != copyFromStore.dbObject, "Missing tracking object. key=%s", key);
                    log.trace("Done Loading key from the store key = {}", copyFromStore.getKey());

                    if (null != copyFromStore.getValue()) {
                        Preconditions.checkState(0 != copyFromStore.getVersion(), "Version is not initialized. key=%s", key);
                    }
                    return insertInBuffer(key, copyFromStore);
                }, executor);
    }

    private CompletableFuture<TransactionData> readFromStore(String key) {
        val fromCache = cache.getIfPresent(key);
        if (null != fromCache) {
            METADATA_FOUND_IN_CACHE.inc();
            return CompletableFuture.completedFuture(fromCache);
        }
        return read(key);
    }

    /**
     * Inserts a copy of metadata into the buffer.
     */
    private TransactionData insertInBuffer(String key, TransactionData copyForBuffer) {
        // If some other transaction beat us then use that value.
        val oldValue = bufferedTxnData.putIfAbsent(key, copyForBuffer);
        final TransactionData retValue;
        if (oldValue != null) {
            retValue = oldValue;
        } else {
            retValue = copyForBuffer;
            bufferCount.incrementAndGet();
        }
        Preconditions.checkState(activeKeys.contains(key), "key must be marked active. Key=%s", key);
        Preconditions.checkState(bufferedTxnData.containsKey(key), "bufferedTxnData must contain the key. Key=%s", key);
        if (!retValue.isPinned()) {
            Preconditions.checkState(null != retValue.dbObject, "Missing tracking object");
        }
        return retValue;
    }

    @VisibleForTesting
    long getBufferCount() {
        return bufferCount.get();
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return A CompletableFuture that, when completed, will contain associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     */
    abstract protected CompletableFuture<TransactionData> read(String key);

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     */
    abstract protected CompletableFuture<Void> writeAll(Collection<TransactionData> dataList);


    /**
     * Retrieve all key-value pairs stored in this instance of {@link ChunkMetadataStore}.
     * There is no order guarantee provided.
     *
     * @return A CompletableFuture that, when completed, will contain {@link Stream} of {@link StorageMetadata} entries.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    abstract public CompletableFuture<Stream<StorageMetadata>> getAllEntries();

    /**
     * Retrieve all keys stored in this instance of {@link ChunkMetadataStore}.
     * There is no order guarantee provided.
     *
     * @return A CompletableFuture that, when completed, will contain {@link Stream} of  {@link String} keys.
     * If the operation failed, it will be completed with the appropriate exception.
     */
    abstract public CompletableFuture<Stream<String>> getAllKeys();

    /**
     * Updates existing metadata.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     *                 throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void update(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn, "txn should not be null.");
        Preconditions.checkArgument(null != metadata, "metadata should not be null.");
        Preconditions.checkArgument(null != metadata.getKey(), "metadata.key should not be null.");
        val txnData = txn.getData();

        val key = metadata.getKey();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }
        data.setValue(metadata);
        data.setPersisted(false);
        Preconditions.checkState(txn.getVersion() >= data.getVersion());
        data.setVersion(txn.getVersion());
    }

    /**
     * Marks given record as pinned.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     *                 throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void markPinned(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn, "txn must not be null.");
        Preconditions.checkArgument(null != metadata, "metadata must not be null.");
        val txnData = txn.getData();
        val key = metadata.getKey();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }

        data.setValue(metadata);
        data.setPinned(true);
        data.setVersion(txn.getVersion());
    }

    /**
     * Creates a new metadata record.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     *                 throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void create(MetadataTransaction txn, StorageMetadata metadata) {
        Preconditions.checkArgument(null != txn, "txn must not be null.");
        Preconditions.checkArgument(null != metadata, "metadata must not be null.");
        Preconditions.checkArgument(null != metadata.getKey(), "metadata.key must not be null.");
        val txnData = txn.getData();
        txnData.put(metadata.getKey(), TransactionData.builder()
                .key(metadata.getKey())
                .value(metadata)
                .version(txn.getVersion())
                .created(true)
                .build());
    }

    /**
     * Deletes a metadata record given the key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     *            throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void delete(MetadataTransaction txn, String key) {
        Preconditions.checkArgument(null != txn, "txn must not be null.");
        Preconditions.checkArgument(null != key, "key must not be null.");
        val txnData = txn.getData();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }
        data.setValue(null);
        data.setPersisted(false);
        data.setDeleted(true);
        data.setVersion(txn.getVersion());
    }


    @Override
    public void report() {
        StorageMetadataMetrics.DYNAMIC_LOGGER.reportGaugeValue(STORAGE_METADATA_BUFFER_SIZE, this.bufferCount);
        StorageMetadataMetrics.DYNAMIC_LOGGER.reportGaugeValue(STORAGE_METADATA_CACHE_SIZE, this.cache.size());
        StorageMetadataMetrics.DYNAMIC_LOGGER.reportGaugeValue(STORAGE_METADATA_CACHE_MISS_RATE, this.cache.stats().missRate());
    }

    /**
     * {@link AutoCloseable#close()} implementation.
     */
    @Override
    public void close() {
        val modifiedValues = new ArrayList<TransactionData>();
        bufferedTxnData.entrySet().stream()
                .filter(entry -> !entry.getValue().isPersisted() && !entry.getValue().isPinned())
                .forEach(entry -> modifiedValues.add(entry.getValue()));
        if (modifiedValues.size() > 0) {
            writeAll(modifiedValues);
        }
    }

    /**
     * Explicitly marks the store as fenced.
     * Once marked fenced no modifications to data should be allowed.
     */
    @Override
    public void markFenced() {
        this.fenced.set(true);
    }

    /**
     * Retrieves the current version number.
     *
     * @return current version number.
     */
    protected long getVersion() {
        return version.get();
    }

    /**
     * Sets the current version number.
     *
     * @param version Version to set.
     */
    protected void setVersion(long version) {
        this.version.set(version);
    }

    /**
     * Stores the transaction data.
     */
    @Builder(toBuilder = true)
    @Data
    public static class TransactionData implements Serializable {

        /**
         * Serializer for {@link StorageMetadata}.
         */
        private final static StorageMetadata.StorageMetadataSerializer SERIALIZER = new StorageMetadata.StorageMetadataSerializer();
        /**
         * Version. This version number is independent of version in the store.
         * This is required to keep track of all modifications to data when it is changed while still in buffer without writing it to database.
         */
        private volatile long version;

        /**
         * Implementation specific object to keep track of underlying db version.
         */
        private volatile Object dbObject;

        /**
         * Whether this record is persisted or not.
         */
        private volatile boolean persisted;

        /**
         * Whether this record is pinned to the memory.
         */
        private volatile boolean pinned;

        /**
         * Whether this record is persisted or not.
         */
        private volatile boolean created;

        /**
         * Whether this record is pinned to the memory.
         */
        private volatile boolean deleted;

        /**
         * Key of the record.
         */
        private volatile String key;

        /**
         * Value of the record.
         */
        private volatile StorageMetadata value;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TransactionDataBuilder implements ObjectBuilder<TransactionData> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class TransactionDataSerializer
                extends VersionedSerializer.WithBuilder<TransactionData, TransactionDataBuilder> {
            @Override
            protected TransactionDataBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput input, TransactionDataBuilder b) throws IOException {
                b.version(input.readLong());
                b.key(input.readUTF());
                val hasValue = input.readBoolean();
                if (hasValue) {
                    b.value(SERIALIZER.deserialize(input.getBaseStream()));
                }
            }

            private void write00(TransactionData object, RevisionDataOutput output) throws IOException {
                output.writeLong(object.version);
                output.writeUTF(object.key);
                val hasValue = object.value != null;
                output.writeBoolean(hasValue);
                if (hasValue) {
                    SERIALIZER.serialize(output, object.value);
                }
            }
        }
    }
}
