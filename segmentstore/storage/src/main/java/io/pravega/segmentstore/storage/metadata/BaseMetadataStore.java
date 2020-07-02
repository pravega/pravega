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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction()}
 *
 * Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload of{@link MetadataTransaction#commit()}.
 * Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.
 * Transactions are atomic - either all changes in the transaction are committed or none at all.
 * In addition, Transactions provide snaphot isolation which means that transaction fails if any of the metadata records read during the transactions are changed outside the transaction after they were read.
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
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    private static final int MAX_ENTRIES_IN_TXN_BUFFER = 5000;

    /**
     * Lock for synchronization.
     */
    private final Object lock = new Object();

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
     */
    @GuardedBy("lock")
    private final ConcurrentHashMap<String, TransactionData> bufferedTxnData;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    @Setter
    int maxEntriesInTxnBuffer = MAX_ENTRIES_IN_TXN_BUFFER;

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
     *
     * @return Returns a new instance of MetadataTransaction.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public MetadataTransaction beginTransaction() throws StorageMetadataException {
        // Each transaction gets a unique number which is monotinically increasing.
        return new MetadataTransaction(this, version.incrementAndGet());
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn, boolean lazyWrite) throws StorageMetadataException {
        commit(txn, lazyWrite, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn transaction to commit.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn) throws StorageMetadataException {
        commit(txn, false, false);
    }

    /**
     * Commits given transaction.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    @Override
    public void commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        if (fenced.get()) {
            throw new StorageMetadataWritesFencedOutException("Transaction writer is fenced off.");
        }

        Map<String, TransactionData> txnData = txn.getData();

        ArrayList<String> modifiedKeys = new ArrayList<>();
        ArrayList<TransactionData> modifiedValues = new ArrayList<>();

        // Step 1 : If bufferedTxnData data was flushed, then read it back from external source and re-insert in bufferedTxnData buffer.
        // This step is kind of thread safe
        for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
            String key = entry.getKey();
            if (skipStoreCheck || entry.getValue().isPinned()) {
                log.trace("Skipping loading key from the store key = {}", key);
            } else {
                // This check is safe to be outside the lock
                if (!bufferedTxnData.containsKey(key)) {
                    loadFromStore(key);
                }
            }
        }
        // Step 2 : Check whether transaction is safe to commit.
        // This check needs to be atomic, with absolutely no possibility of re-entry
        synchronized (lock) {
            for (Map.Entry<String, TransactionData> entry : txnData.entrySet()) {
                String key = entry.getKey();
                val transactionData = entry.getValue();
                Preconditions.checkState(null != transactionData.getKey());

                // See if this entry was modified in this transaction.
                if (transactionData.getVersion() == txn.getVersion()) {
                    modifiedKeys.add(key);
                    transactionData.setPersisted(false);
                    modifiedValues.add(transactionData);
                }
                // make sure none of the keys used in this transaction have changed.
                TransactionData dataFromBuffer = bufferedTxnData.get(key);
                if (null != dataFromBuffer) {
                    if (dataFromBuffer.getVersion() > transactionData.getVersion()) {
                        throw new StorageMetadataVersionMismatchException(
                                String.format("Transaction uses stale data. Key version changed key:%s buffer:%s transaction:%s",
                                        key, dataFromBuffer.getVersion(), txnData.get(key).getVersion()));
                    }

                    // Pin it if it is already pinned.
                    transactionData.setPinned(transactionData.isPinned() || dataFromBuffer.isPinned());

                    // Set the database object.
                    transactionData.setDbObject(dataFromBuffer.getDbObject());
                }
            }

            // Step 3: Commit externally.
            // This operation may call external storage.
            if (!lazyWrite || (bufferedTxnData.size() > maxEntriesInTxnBuffer)) {
                log.trace("Persisting all modified keys (except pinned)");
                val toWriteList = modifiedValues.stream().filter(entry -> !entry.isPinned()).collect(Collectors.toList());
                writeAll(toWriteList);
                log.trace("Done persisting all modified keys");

                // Mark written keys as persisted.
                for (val writtenData : toWriteList) {
                    writtenData.setPersisted(true);
                }
            }

            // Execute external commit step.
            try {
                if (null != txn.getExternalCommitStep()) {
                    txn.getExternalCommitStep().call();
                }
            } catch (Exception e) {
                log.error("Exception during execution of external commit step", e);
                throw new StorageMetadataException("Exception during execution of external commit step", e);
            }

            // If we reach here then it means transaction is safe to commit.
            // Step 4: Insert
            long committedVersion = version.incrementAndGet();
            HashMap<String, TransactionData> toAdd = new HashMap<String, TransactionData>();
            for (String key : modifiedKeys) {
                TransactionData data = txnData.get(key);
                data.setVersion(committedVersion);
                toAdd.put(key, data);
            }
            bufferedTxnData.putAll(toAdd);
        }

        //  Step 5 : evict if required.
        if (bufferedTxnData.size() > maxEntriesInTxnBuffer) {
            bufferedTxnData.entrySet().removeIf(entry -> entry.getValue().isPersisted() && !entry.getValue().isPinned());
        }

        //  Step 6: finally clear
        txnData.clear();
    }

    /**
     * Aborts given transaction.
     *
     * @param txn transaction to abort.
     * @throws StorageMetadataException If there are any errors.
     */
    public void abort(MetadataTransaction txn) throws StorageMetadataException {
        // Do nothing
    }

    /**
     * Retrieves the metadata for given key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @return Metadata for given key. Null if key was not found.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public StorageMetadata get(MetadataTransaction txn, String key) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        TransactionData dataFromBuffer = null;
        if (null == key) {
            return null;
        }
        StorageMetadata retValue = null;

        Map<String, TransactionData> txnData = txn.getData();
        TransactionData data = txnData.get(key);

        // Search in the buffer.
        if (null == data) {
            synchronized (lock) {
                dataFromBuffer = bufferedTxnData.get(key);
            }
            // If we did not find in buffer then load it from store
            if (null == dataFromBuffer) {
                // NOTE: This call to read MUST be outside the lock, it is most likely cause re-entry.
                loadFromStore(key);
                dataFromBuffer = bufferedTxnData.get(key);
                Preconditions.checkState(null != dataFromBuffer);
            }

            if (null != dataFromBuffer && null != dataFromBuffer.getValue()) {
                // Make copy.
                data = dataFromBuffer.toBuilder()
                        .key(key)
                        .value(dataFromBuffer.getValue().deepCopy())
                        .build();
                txnData.put(key, data);
            }
        }

        if (data != null) {
            retValue = data.getValue();
        }

        return retValue;
    }

    /**
     * Loads value from store
     *
     * @param key Key to load
     * @return Value if found null otherwise.
     * @throws StorageMetadataException Any exceptions.
     */
    private TransactionData loadFromStore(String key) throws StorageMetadataException {
        // NOTE: This call to read MUST be outside the lock, it is most likely cause re-entry.
        log.trace("Loading key from the store key = {}", key);
        TransactionData fromDb = read(key);
        Preconditions.checkState(null != fromDb);
        log.trace("Done Loading key from the store key = {}", key);

        TransactionData copyForBuffer = fromDb.toBuilder()
                .key(key)
                .build();

        if (null != fromDb.getValue()) {
            Preconditions.checkState(0 != fromDb.getVersion(), "Version is not initialized");
            // Make sure it is a deep copy.
            copyForBuffer.setValue(fromDb.getValue().deepCopy());
        }
        // Put this value in bufferedTxnData buffer.
        synchronized (lock) {
            // If some other transaction beat us then use that value.
            TransactionData oldValue = bufferedTxnData.putIfAbsent(key, copyForBuffer);
            if (oldValue != null) {
                copyForBuffer = oldValue;
            }
        }
        return copyForBuffer;
    }

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return Associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    abstract protected TransactionData read(String key) throws StorageMetadataException;

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    abstract protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException;

    /**
     * Updates existing metadata.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void update(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Preconditions.checkArgument(null != metadata.getKey());
        Map<String, TransactionData> txnData = txn.getData();

        String key = metadata.getKey();

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
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void markPinned(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Map<String, TransactionData> txnData = txn.getData();
        String key = metadata.getKey();

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
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void create(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != metadata);
        Preconditions.checkArgument(null != metadata.getKey());
        Map<String, TransactionData> txnData = txn.getData();
        txnData.put(metadata.getKey(), TransactionData.builder()
                .key(metadata.getKey())
                .value(metadata)
                .version(txn.getVersion())
                .build());
    }

    /**
     * Deletes a metadata record given the key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    @Override
    public void delete(MetadataTransaction txn, String key) throws StorageMetadataException {
        Preconditions.checkArgument(null != txn);
        Preconditions.checkArgument(null != key);
        Map<String, TransactionData> txnData = txn.getData();

        TransactionData data = TransactionData.builder().key(key).build();
        TransactionData oldData = txnData.putIfAbsent(key, data);
        if (null != oldData) {
            data = oldData;
        }
        data.setValue(null);
        data.setPersisted(false);
        data.setVersion(txn.getVersion());
    }

    /**
     * {@link AutoCloseable#close()} implementation.
     */
    @Override
    public void close() throws Exception {
        ArrayList<TransactionData> modifiedValues = new ArrayList<>();
        bufferedTxnData.entrySet().stream().filter(entry -> !entry.getValue().isPersisted() && !entry.getValue().isPinned()).forEach(entry -> modifiedValues.add(entry.getValue()));
        if (modifiedValues.size() > 0) {
            writeAll(modifiedValues);
        }
    }

    /**
     * Explicitly marks the store as fenced.
     * Once marked fenced no modifications to data should be allowed.
     */
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
        private long version;

        /**
         * Implementation specific object to keep track of underlying db version.
         */
        private Object dbObject;

        /**
         * Whether this record is persisted or not.
         */
        private boolean persisted;

        /**
         * Whether this record is pinned to the memory.
         */
        private boolean pinned;

        /**
         * Key of the record.
         */
        private String key;

        /**
         * Value of the record.
         */
        private StorageMetadata value;

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
                boolean hasValue = input.readBoolean();
                if (hasValue) {
                    b.value(SERIALIZER.deserialize(input.getBaseStream()));
                }
            }

            private void write00(TransactionData object, RevisionDataOutput output) throws IOException {
                output.writeLong(object.version);
                output.writeUTF(object.key);
                boolean hasValue = object.value != null;
                output.writeBoolean(hasValue);
                if (hasValue) {
                    SERIALIZER.serialize(output, object.value);
                }
            }
        }
    }
}
