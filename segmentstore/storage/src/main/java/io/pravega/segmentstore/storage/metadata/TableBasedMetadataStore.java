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
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TableBasedMetadataStore extends BaseMetadataStore {
    TableStore tableStore;
    String tableName;
    Duration timeout = Duration.ofSeconds(1L);
    AtomicBoolean isTableInitialized = new AtomicBoolean(false);

    public TableBasedMetadataStore(String tableName, TableStore tableStore) {
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    }

    @Override
    protected TransactionData read( String key) throws StorageMetadataException {
        ensureInitialized();
        List<ArrayView> keys = new ArrayList<>();
        keys.add(new ByteArraySegment(key.getBytes()));
        try {
            List<TableEntry> retValue = this.tableStore.get(tableName, keys, timeout).get();
            if (retValue.size() == 1) {
                TableEntry entry = retValue.get(0);
                if (null != entry) {
                    val arr = entry.getValue();
                    ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(arr.array(), arr.arrayOffset(), arr.getLength()));
                    StorageMetadata metadata = (StorageMetadata) input.readObject();
                    TransactionData txnData = TransactionData.builder()
                            .value(metadata)
                            .dbObject(entry.getKey().getVersion())
                            .deleted(false)
                            .modified(false)
                            .build();
                    return txnData;
                }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageMetadataException("Error while reading", e);
        }
        return null;
    }

    @Override
    protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException {
        ensureInitialized();
        List<TableEntry> toUpdate = new ArrayList<>();
        HashMap<TableEntry, TransactionData> map = new HashMap<TableEntry, TransactionData>();
        List<TableKey> keysToDelete = new ArrayList<>();
        try {
            for (TransactionData txnData : dataList) {
                StorageMetadata metadata = txnData.getValue();
                long version = TableKey.NOT_EXISTS;
                if (null != txnData.getDbObject()) {
                    version = ((Long) txnData.getDbObject()).longValue();
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos);

                if (txnData.isDeleted()) {
                    keysToDelete.add(TableKey.versioned(new ByteArraySegment(metadata.getKey().getBytes()),
                            TableKey.NO_VERSION)); //version));
                    out.writeObject(null);
                } else {
                    out.writeObject(metadata);
                }

                out.flush();
                val bytes = bos.toByteArray();

                TableEntry tableEntry = TableEntry.versioned(
                        new ByteArraySegment(metadata.getKey().getBytes()),
                        new ByteArraySegment(bytes),
                        version);
                map.put(tableEntry, txnData);
                toUpdate.add(tableEntry);
            }

            // Now put uploaded keys.
            List<Long> ret = this.tableStore.put(tableName, toUpdate, timeout).get();

            // Delete deleted keys.
            int i = 0;
            for (TableEntry tableEntry : toUpdate) {
                map.get(tableEntry).setDbObject(ret.get(i));
                i++;
            }
            this.tableStore.remove(tableName, keysToDelete, timeout).get();
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
