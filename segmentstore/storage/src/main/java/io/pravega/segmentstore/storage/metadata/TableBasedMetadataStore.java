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

import com.google.common.base.Preconditions;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link TableStore} based storage metadata store.
 */
@Slf4j
public class TableBasedMetadataStore extends BaseMetadataStore {
    private final TableStore tableStore;
    private final String tableName;
    private final Duration timeout = Duration.ofSeconds(1L);
    private final AtomicBoolean isTableInitialized = new AtomicBoolean(false);
    private final BaseMetadataStore.TransactionData.TransactionDataSerializer serializer = new BaseMetadataStore.TransactionData.TransactionDataSerializer();

    /**
     * Constructor.
     *
     * @param tableName Name of the table segment.
     * @param tableStore Instance of the {@link TableStore}.
     */
    public TableBasedMetadataStore(String tableName, TableStore tableStore) {
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return Associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    protected TransactionData read(String key) throws StorageMetadataException {
        ensureInitialized();
        List<BufferView> keys = new ArrayList<>();
        keys.add(new ByteArraySegment(key.getBytes()));
        try {
            List<TableEntry> retValue = this.tableStore.get(tableName, keys, timeout).get();
            if (retValue.size() == 1) {
                TableEntry entry = retValue.get(0);
                if (null != entry) {
                    val arr = entry.getValue();
                    TransactionData txnData = serializer.deserialize(arr);
                    txnData.setDbObject(entry.getKey().getVersion());
                    txnData.setPersisted(true);
                    return txnData;
                }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageMetadataException("Error while reading", e);
        }

        return TransactionData.builder()
                .key(key)
                .persisted(true)
                .dbObject(TableKey.NOT_EXISTS)
                .build();
    }

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException {
        ensureInitialized();
        List<TableEntry> toUpdate = new ArrayList<>();
        HashMap<TableEntry, TransactionData> entryToTxnDataMap = new HashMap<TableEntry, TransactionData>();
        HashMap<TableKey, TransactionData> deletedKeyToTxnDataMap = new HashMap<TableKey, TransactionData>();
        List<TableKey> keysToDelete = new ArrayList<>();
        try {
            for (TransactionData txnData : dataList) {
                Preconditions.checkState(null != txnData.getDbObject());

                long version = ((Long) txnData.getDbObject()).longValue();
                if (null == txnData.getValue()) {
                    val toDelete = TableKey.versioned(new ByteArraySegment(txnData.getKey().getBytes()),
                            TableKey.NO_VERSION);
                    keysToDelete.add(toDelete);
                    deletedKeyToTxnDataMap.put(toDelete, txnData);
                }

                val arraySegment = serializer.serialize(txnData);

                TableEntry tableEntry = TableEntry.versioned(
                        new ByteArraySegment(txnData.getKey().getBytes()),
                        arraySegment,
                        version);
                entryToTxnDataMap.put(tableEntry, txnData);
                toUpdate.add(tableEntry);
            }

            // Now put uploaded keys.
            List<Long> ret = this.tableStore.put(tableName, toUpdate, timeout).get();

            // Update versions.
            int i = 0;
            for (TableEntry tableEntry : toUpdate) {
                entryToTxnDataMap.get(tableEntry).setDbObject(ret.get(i));
                i++;
            }

            // Delete deleted keys.
            this.tableStore.remove(tableName, keysToDelete, timeout).get();
            for (val deletedKey : keysToDelete) {
                deletedKeyToTxnDataMap.get(deletedKey).setDbObject(TableKey.NOT_EXISTS);
            }
        } catch (RuntimeException e) {
            throw e; // To make spotbugs happy.
        } catch (java.util.concurrent.ExecutionException e) {
            handleException(e.getCause());
            return;
        } catch (Exception e) {
            handleException(e);
            return;
        }

    }

    private void handleException(Throwable e) throws StorageMetadataException {
        if (e instanceof DataLogWriterNotPrimaryException) {
            throw new StorageMetadataWritesFencedOutException("Transaction failed. Writer fenced off", e);
        }
        if (e instanceof BadKeyVersionException) {
            throw new StorageMetadataVersionMismatchException("Transaction failed. Version Mismatch.", e);
        }
        if (e.getCause() != null) {
            if (e.getCause().getCause() instanceof BadKeyVersionException) {
                throw new StorageMetadataWritesFencedOutException("Transaction writer is fenced off.", e);
            }
            if (e.getCause().getCause() instanceof DataLogWriterNotPrimaryException) {
                throw new StorageMetadataVersionMismatchException("Transaction failed. Writer fenced off", e);
            }
        } else {
            log.debug("e.getCause()=null", e);
        }
        throw new StorageMetadataException("Transaction failed", e);
    }

    private void ensureInitialized() {
        if (!isTableInitialized.get()) {
            try {
                this.tableStore.createSegment(tableName, timeout).join();
                log.info("Created table segment {}", tableName);
            } catch (CompletionException e) {
                if (e.getCause() instanceof StreamSegmentExistsException) {
                    log.info("Table segment {} already exists.", tableName);
                }
            }
            isTableInitialized.set(true);
        }
    }
}
