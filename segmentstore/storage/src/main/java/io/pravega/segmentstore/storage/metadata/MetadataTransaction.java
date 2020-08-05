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

import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the storage metadata transaction.
 * All access to and modifications to the metadata the {@link ChunkMetadataStore} must be done through a transaction.
 * This implementation delegates all calls to underlying {@link ChunkMetadataStore}.
 *
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction()}
 * <ul>
 * <li>Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload
 * of{@link MetadataTransaction#commit()}.</li>
 * <li>Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.</li>
 * <li>Transactions are atomic - either all changes in the transaction are committed or none at all.</li>
 * <li>In addition, Transactions provide snaphot isolation which means that transaction fails if any of the metadata
 * records read during the transactions are changed outside the transaction after they were read.</li>
 * </ul>
 *
 * Within a transaction you can perform following actions on a per record basis.
 * <ul>
 * <li>{@link MetadataTransaction#get(String)} Retrieves metadata using for given key.</li>
 * <li>{@link MetadataTransaction#create(StorageMetadata)} Creates a new record.</li>
 * <li>{@link MetadataTransaction#delete(String)} Deletes records for given key.</li>
 * <li>{@link MetadataTransaction#update(StorageMetadata)} Updates the transaction local copy of the record.
 * For each record modified inside the transaction update must be called to mark the record as dirty.</li>
 * </ul>
 *
 * Underlying implementation might buffer frequently or recently updated metadata keys to optimize read/write performance.
 * To further optimize it may provide "lazy committing" of changes where there is application specific way to recover
 * from failures.(Eg. when only length of chunk is changed.)
 * In this case {@link MetadataTransaction#commit(boolean)} can be called.Note that otherwise for each commit the data
 * is written to underlying key-value store.
 *
 * There are two special methods provided to handle metadata about data segments for the underlying key-value store.
 * They are useful in avoiding circular references.
 * <ul>
 * <li>A record marked as pinned by calling {@link MetadataTransaction#markPinned(StorageMetadata)} is never written to
 * underlying storage.</li>
 * <li>In addition transaction can be committed using {@link MetadataTransaction#commit(boolean, boolean)} to skip
 * validation step that reads any recently evicted changes from underlying storage.</li>
 * </ul>
 */
@NotThreadSafe
public class MetadataTransaction implements AutoCloseable {
    /**
     * {@link ChunkMetadataStore} that stores the actual metadata.
     */
    private final ChunkMetadataStore store;

    /**
     * Indicates whether the transaction is commited or not.
     */
    @Getter
    private boolean isCommitted = false;

    /**
     * Indicates whether the transaction is aborted or not.
     */
    @Getter
    private boolean isAborted = false;

    /**
     * The version of the transaction.
     */
    @Getter
    private final long version;

    /**
     * Local data in the transaction.
     */
    @Getter
    private final ConcurrentHashMap<String, BaseMetadataStore.TransactionData> data;

    /**
     * Optional external commit operation that is executed during the commit.
     * The transaction commit operation fails if this operation fails.
     */
    @Getter
    @Setter
    private Callable<Void> externalCommitStep;

    /**
     * Constructor.
     *
     * @param store   Underlying metadata store.
     * @param version Version number of the transactions.
     */
    public MetadataTransaction(ChunkMetadataStore store, long version) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.version = version;
        data = new ConcurrentHashMap<String, BaseMetadataStore.TransactionData>();
    }

    /**
     * Retrieves the metadata for given key.
     *
     * @param key key to use to retrieve metadata.
     * @return Metadata for given key. Null if key was not found.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    public StorageMetadata get(String key) throws StorageMetadataException {
        return store.get(this, key);
    }

    /**
     * Updates existing metadata.
     *
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    public void update(StorageMetadata metadata) throws StorageMetadataException {
        store.update(this, metadata);
    }

    /**
     * Creates a new metadata record.
     *
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    public void create(StorageMetadata metadata) throws StorageMetadataException {
        store.create(this, metadata);
    }

    /**
     * Marks given record as pinned.
     *
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    public void markPinned(StorageMetadata metadata) throws StorageMetadataException {
        store.markPinned(this, metadata);
    }

    /**
     * Deletes a metadata record given the key.
     *
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    public void delete(String key) throws StorageMetadataException {
        store.delete(this, key);
    }

    /**
     * Commits the transaction.
     *
     * @throws StorageMetadataException If transaction could not be commited.
     */
    public void commit() throws StorageMetadataException {
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        store.commit(this);
        isCommitted = true;
    }

    /**
     * Commits the transaction.
     *
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException If transaction could not be commited.
     */
    public void commit(boolean lazyWrite) throws StorageMetadataException {
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        store.commit(this, lazyWrite);
        isCommitted = true;
    }

    /**
     * Commits the transaction.
     *
     * @param lazyWrite      true if data can be written lazily.
     * @param skipStoreCheck true if data is not to be reloaded from store.
     * @throws StorageMetadataException If transaction could not be commited.
     */
    public void commit(boolean lazyWrite, boolean skipStoreCheck) throws StorageMetadataException {
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        store.commit(this, lazyWrite, skipStoreCheck);
        isCommitted = true;
    }

    /**
     * Aborts the transaction.
     *
     * @throws StorageMetadataException If transaction could not be commited.
     */
    public void abort() throws StorageMetadataException {
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        isAborted = true;
        store.abort(this);
    }

    /**
     * {@link AutoCloseable#close()} implementation.
     *
     * @throws Exception In case of any errors.
     */
    public void close() throws Exception {
        if (!isCommitted || isAborted) {
            store.abort(this);
        }
    }
}

