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

/**
 * Storage Metadata store.
 */
public interface ChunkMetadataStore extends AutoCloseable {
    /**
     * Begins a new transaction.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     * @return
     *
     */
    MetadataTransaction beginTransaction() throws StorageMetadataException;

    /**
     * Retrieves the metadata for given key.
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     * @return
     */
    StorageMetadata get(MetadataTransaction txn, String key) throws StorageMetadataException;

    /**
     * Updates existing metadata.
     * @param txn Transaction.
     * @param metadata metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void update(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException;

    /**
     * Creates a new metadata record.
     * @param txn Transaction.
     * @param metadata  metadata record.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void create(MetadataTransaction txn, StorageMetadata metadata) throws StorageMetadataException;

    /**
     * Deletes a metadata record given the key.
     * @param txn Transaction.
     * @param key key to use to retrieve metadata.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    void delete(MetadataTransaction txn, String key) throws StorageMetadataException;

    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @param skipStoreCheck true if data is not to be reloaded from store.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn, boolean lazyWrite, boolean skipStoreCheck) throws StorageMetadataException;

    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @param lazyWrite true if data can be written lazily.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn, boolean lazyWrite) throws StorageMetadataException;


    /**
     * Commits given transaction.
     * @param txn transaction to commit.
     * @throws StorageMetadataException StorageMetadataVersionMismatchException if transaction can not be commited.
     */
    void commit(MetadataTransaction txn) throws StorageMetadataException;

    /**
     * Aborts given transaction.
     * @param txn transaction to abort.
     * @throws StorageMetadataException If there are any errors.
     */
    void abort(MetadataTransaction txn) throws StorageMetadataException;

    /**
     * Fences the store.
     */
    void markFenced();
}
