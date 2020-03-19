/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.metadata;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements Base metadata store.
 */
@Slf4j
public class BaseMetadataStore implements ChunkMetadataStore {
    private static final int MAX_ENTRIES_IN_TXN_BUFFER = 5000; //TODO : load from the config
    private final Object lock = new Object();
    private final AtomicBoolean fenced;
    /**
     *  Monotonically increasing number. Keeps track of versions independent of external persistence or transaction mechanism.
     */
    private final AtomicLong version;

    /**
     * Buffer for reading and writting transaction data entries to underlying KV store.
     * This allows lazy storing and avoiding unnecessary load for recently/frequently updated key value pairs.
     */
    @GuardedBy("lock")
    private final ConcurrentHashMap<String, TransactionData> bufferedTxnData;

    @Getter
    @Setter
    int maxCacheSize = MAX_ENTRIES_IN_TXN_BUFFER;

    /**
     * Constructs a BaseMetadataStore object.
     */
    public BaseMetadataStore() {
        version = new AtomicLong(System.currentTimeMillis()); // Start with unique number.
        fenced = new AtomicBoolean(false);
        bufferedTxnData = new ConcurrentHashMap<>(); // Don't think we need anything fancy here. But we'll measure and see.
    }

    /**
     * Begins a new transaction.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     * @return
     *
     */
    @Override
    public MetadataTransaction beginTransaction() throws StorageMetadataException {
        return new MetadataTransaction(this);
    }

    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn, boolean lazyWrite) throws StorageMetadataException  {
        commit(txn, lazyWrite, false);
    }

    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn) throws StorageMetadataException {
        commit(txn, false, false);
    }

    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) throws StorageMetadataException  {
        if (fenced.get()) {
            throw new StorageMetadataWritesFencedOutException("Transaction writer is fenced off.");
        }

        Map<String, TransactionData> txnData = txn.getData();

        ArrayList<String> modifiedKeys = new ArrayList<>();
        ArrayList<TransactionData> modifiedValues = new ArrayList<>();

        // Step 1 : If bufferedTxnData data was flushed, then read it back from external source and re-insert in bufferedTxnData cache.
        // This step is kind of thread safe
        for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
            String key = entry.getKey();
            if (skipStoreCheck) {
                log.debug("Skipping loading key from the store key = {}", key);
            } else {
                // This check is safe to be outside the lock
                if (!bufferedTxnData.containsKey(key)) {

                    // NOTE: This call to read MUST be outside the lock, it is most likely cause re-entry.
                    log.debug("Loading key from the store key = {}", key);
                    TransactionData fromDb = read(key);
                    log.debug("Done Loading key from the store key = {}", key);
                    if (null != fromDb) {
                        // If some other transaction beat us then use that value.
                        // If any transaction was committed after we checked then it must have latest value,
                        // otherwise the bufferedTxnData will have same value. so ignore what was retrieved.
                        synchronized (lock) {
                            bufferedTxnData.putIfAbsent(key, fromDb);
                        }
                    }
                }
            }

        }

        // Step 2 : Check whether transaction is safe to commit.
        // This check needs to be atomic, with absolutely no possibility of re-entry
        synchronized (lock) {
            for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
                String key = entry.getKey();
                if (entry.getValue().isModified() || entry.getValue().isDeleted()) {
                    modifiedKeys.add(key);
                    modifiedValues.add(entry.getValue());
                }
                // make sure none of the keys used in this transaction have changed.
                TransactionData globalData = bufferedTxnData.get(key);
                if (bufferedTxnData.containsKey(key)) {
                    //if (bufferedTxnData.get(key).isDeleted() && !txnData.get(key).isDeleted()) {
                    if (bufferedTxnData.get(key).isDeleted()) {
                        // allow create after delete.
                        continue;
                    }
                    if (bufferedTxnData.get(key).getVersion() > txnData.get(key).getVersion()) {
                        throw new StorageMetadataVersionMismatchException(
                                String.format("Transaction uses stale data. Key version changed key:{} expected:{} actual:{}",
                                        key, bufferedTxnData.get(key).getVersion(), txnData.get(key).getVersion()));
                    }
                }
            }

            // If we reach here then it means transaction is safe to commit.

            // Step 3: Insert
            long committedVersion = version.incrementAndGet();
            HashMap<String, TransactionData> toAdd = new HashMap<String, TransactionData>();
            for (String key : modifiedKeys) {
                TransactionData data = txnData.get(key);
                data.setVersion(committedVersion);
                data.setPersisted(false);
                toAdd.put(key, data);
            }
            bufferedTxnData.putAll(toAdd);
        }

        //  Step 4 : evict cache if required.
        //if (bufferedTxnData.size() > maxCacheSize) {
        //    bufferedTxnData.entrySet().removeIf(entry -> entry.getValue().isPersisted());
        //}

        // Step 4: Commit externally.
        // This operation may call external storage which might end up in re-entrant call, so don't have any locks here.
        if (!lazyWrite || (bufferedTxnData.size() > maxCacheSize)) {
            log.debug("Persisting all modified keys");
            writeAll(modifiedValues);
            log.debug("Done persisting all modified keys");
        }
        // Mark written keys as persisted.
        for (String key: modifiedKeys) {
            TransactionData data = bufferedTxnData.get(key);
            data.setPersisted(true);
        }

        //  Step 5 : evict cache if required.
        if (bufferedTxnData.size() > maxCacheSize) {
            bufferedTxnData.entrySet().removeIf(entry -> entry.getValue().isPersisted());
        }

        //  Step 6: finally clear
        txnData.clear();
    }

    /**
     * Aborts given transaction.
     * @param txn transaction to abort.
     * @throws StorageMetadataException If there are any errors.
     */
    public void abort(MetadataTransaction txn) throws StorageMetadataException  {
    }

    /**
     * Retrieves the metadata for given key.
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     * @return Metadata for given key.
     */
    @Override
    public StorageMetadata get(MetadataTransaction txn, String key) throws StorageMetadataException  {
        if (null != key) {
            Map<String, TransactionData> txnData = txn.getData();
            TransactionData data = null;

            if (txnData.containsKey(key)) {
                data = txnData.get(key);
            } else {
                TransactionData globalData = null;
                synchronized (lock) {
                    globalData = bufferedTxnData.get(key);
                }
                if (null != globalData) {
                    data = globalData.toBuilder().value(globalData.getValue().copy()).build();
                    txnData.put(key, data);
                }
            }

            if (data != null) {
                if (data.isDeleted()) {
                    return null;
                }
                return data.getValue();
            }

            // NOTE: This call to read MUST be outside the lock, it is most likely cause re-entry.
            TransactionData retValue = null;
            try {
                retValue = read(key);
            } catch (StorageMetadataNotFoundException ex) {
                log.debug("Key not found in store.");
            }

            if (null != retValue
                && !retValue.isDeleted()
                && null != retValue.getValue()) {
                retValue.setVersion(version.get());

                // Put this value in bufferedTxnData cache.
                synchronized (lock) {
                    TransactionData oldValue = bufferedTxnData.putIfAbsent(key, retValue);
                    if (oldValue != null) {
                        retValue = oldValue;
                    }
                }

                // Also put this in transaction local view.
                // Make copy so that bufferedTxnData data is not affected by update inside the transactions.
                txnData.put(key, TransactionData.builder()
                        .value(retValue.getValue())
                        .dbObject(retValue.getDbObject())
                        .version(retValue.getVersion())
                        .modified(true)
                        .build());
                return retValue.getValue();
            }
        }
        return null;
    }

    protected TransactionData read( String key) throws StorageMetadataException {
        return null;
    }

    protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException {
    }

    /**
     * Updates existing metadata.
     * @param txn Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void update(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException  {
        if (metadata == null) {
            return;
        }
        Map<String, TransactionData> txnData = txn.getData();
        synchronized (lock) {
            TransactionData data = bufferedTxnData.get(metadata.getKey());

            txnData.put(metadata.getKey(), TransactionData.builder()
                    .value(metadata)
                    .dbObject(data == null ? null : data.getDbObject())
                    .version(data == null ? 0 : data.getVersion())
                    .modified(true)
                    .isPersisted(false)
                    .build());
        }
    }

    /**
     * Creates a new metadata record.
     * @param txn Transaction.
     * @param metadata  metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void create(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException  {
        Map<String, TransactionData> txnData = txn.getData();
        txnData.put(metadata.getKey(), TransactionData.builder().value(metadata).modified(true).build() );
    }

    /**
     * Deletes a metadata record given the key.
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void delete(MetadataTransaction txn, String key) throws StorageMetadataException {
        Map<String, TransactionData> txnData = txn.getData();
        TransactionData data;
        if (txnData.containsKey(key)) {
            data = txnData.get(key);
            data.setDeleted(true);
            data.setPersisted(false);
        } else {
            synchronized (lock) {
                if (bufferedTxnData.containsKey(key)) {
                    data = bufferedTxnData.get(key);
                    txnData.put(key, TransactionData.builder()
                            .value(data.getValue())
                            .dbObject(data == null ? null : data.getDbObject())
                            .version(data == null ? 0 : data.getVersion())
                            .deleted(true)
                            .modified(true)
                            .isPersisted(false)
                            .build());
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        ArrayList<TransactionData> modifiedValues = new ArrayList<>();
        bufferedTxnData.entrySet().stream().filter(entry -> !entry.getValue().isPersisted()).forEach(entry -> modifiedValues.add(entry.getValue()));
        if (modifiedValues.size() > 0) {
            writeAll(modifiedValues);
        }
    }

    /**
     * Marks the store as fenced.
     */
    public void markFenced() {
        this.fenced.set(true);
    }

    /**
     * Stores the transaction data.
     */
    @Builder(toBuilder = true)
    @Data
    public static class TransactionData {
        private long version;
        private Object dbObject;
        private boolean deleted;
        private boolean modified;
        private boolean isPersisted;
        private StorageMetadata value;
    }
}
