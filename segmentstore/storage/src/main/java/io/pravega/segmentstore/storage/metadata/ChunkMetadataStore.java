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

/**
 * Storage Metadata store.
 * All storage related metadata is stored using set of key-value pairs.
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 *
 * Metadata can any type that implements {@link StorageMetadata}
 * The String value returned by {@link StorageMetadata#getKey()} is used as key.
 *
 * All access to and modifications to the metadata the {@link ChunkMetadataStore} must be done through a transaction.
 *
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction()}
 *
 * Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload of
 * {@link MetadataTransaction#commit()}.
 * Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.
 * Transactions are atomic - either all changes in the transaction are committed or none at all.
 * In addition, Transactions provide snaphot isolation which means that transaction fails if any of the metadata records
 * read during the transactions are changed outside the transaction after they were read.
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
 * To further optimize it may provide "lazy committing" of changes where there is application specific way to recover
 * from failures.(Eg. when only length of chunk is changed.)
 * In this case {@link ChunkMetadataStore#commit(MetadataTransaction, boolean)} can be called. The data is not written to the underlying storage.
 * Note that otherwise for each commit the data is written to underlying key-value store.
 *
 * There are two special methods provided to handle metadata about data segments for the underlying key-value store.
 * They are useful in avoiding circular references.
 * <ul>
 * <li>A record marked as pinned by calling {@link MetadataTransaction#markPinned(StorageMetadata)} is never written to
 * underlying storage.</li>
 * <li>A transaction can be committed using {@link ChunkMetadataStore#commit(MetadataTransaction, boolean, boolean)} to
 * skip validation step that reads any recently evicted changes from underlying storage.</li>
 * </ul>
 */
@Beta
public interface ChunkMetadataStore extends AutoCloseable {
    /**
     * Begins a new transaction.
     *
     * @param keysToLock Array of keys to lock for this transaction.
     * @return Returns a new instance of {@link MetadataTransaction}.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    MetadataTransaction beginTransaction(String... keysToLock) throws StorageMetadataException;

    /**
     * Retrieves the metadata for given key.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @return Metadata for given key. Null if key was not found.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    StorageMetadata get(MetadataTransaction txn, String key) throws StorageMetadataException;

    /**
     * Updates existing metadata.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void update(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException;

    /**
     * Creates a new metadata record.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void create(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException;

    /**
     * Marks given single record as pinned.
     * Pinned records are not evicted from memory and are not written to the underlying storage.
     *
     * @param txn      Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void markPinned(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException;

    /**
     * Deletes a metadata record given the key.
     * The transaction data is validated and changes are commited to underlying storage.
     * This call blocks until write to underlying storage is confirmed.
     *
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void delete(MetadataTransaction txn, String key) throws StorageMetadataException;

    /**
     * Commits given transaction.
     * If  skipStoreCheck is set to true then the transaction data is validated without realoding.
     * This call blocks until write to underlying storage is confirmed. This helps avoid circular dependency on storage
     * system segments.
     * If lazyWrite is true then the transaction data is validated but the changes are not commited to underlying storage.
     * Changes are put in the in memory buffer only. Note that in case of crash, the changes in the in buffer are lost.
     * In this case the state must be re-created using application specific recovery/failover logic.
     * Do not commit lazily if such recovery is not possible.
     * This call does not blocks until write to underlying storage is confirmed if lazyWrite is true.
     *
     * @param txn            transaction to commit.
     * @param lazyWrite      true if data can be written lazily.
     * @param skipStoreCheck true if data is not to be reloaded from store.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) throws StorageMetadataException;

    /**
     * Commits given transaction.
     * If lazyWrite is true then the transaction data is validated but the changes are not commited to underlying storage.
     * Changes are put in the in memory buffer only. Note that in case of crash, the changes in the in buffer are lost.
     * In this case the state must be re-created using application specific recovery/failover logic.
     * Do not commit lazily if such recovery is not possible.
     * This call does not blocks until write to underlying storage is confirmed if lazyWrite is true.
     *
     * @param txn       transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn, boolean lazyWrite) throws StorageMetadataException;

    /**
     * Commits given transaction.
     *
     * @param txn transaction to commit.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn) throws StorageMetadataException;

    /**
     * Aborts given transaction.
     *
     * @param txn transaction to abort.
     * @throws StorageMetadataException If there are any errors.
     */
    void abort(MetadataTransaction txn) throws StorageMetadataException;

    /**
     * Explicitly marks the store as fenced.
     * Once marked fenced no modifications to data should be allowed.
     */
    void markFenced();
}
