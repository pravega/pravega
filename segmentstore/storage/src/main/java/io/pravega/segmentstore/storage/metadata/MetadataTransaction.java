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

import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the storage metadata transaction.
 * All access to and modifications to the metadata the {@link ChunkMetadataStore} must be done through a transaction.
 * This implementation delegates all calls to underlying {@link ChunkMetadataStore}.
 *
 * A transaction is created by calling {@link ChunkMetadataStore#beginTransaction(boolean, String...)}
 * <ul>
 * <li>Changes made to metadata inside a transaction are not visible until a transaction is committed using any overload
 * of{@link MetadataTransaction#commit()}.</li>
 * <li>Transaction is aborted automatically unless committed or when {@link MetadataTransaction#abort()} is called.</li>
 * <li>Transactions are atomic - either all changes in the transaction are committed or none at all.</li>
 * <li>In addition, Transactions provide snapshot isolation which means that transaction fails if any of the metadata
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
     * Indicates whether the transaction is committed or not.
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
     * Whether the transaction is readonly or not.
     */
    @Getter
    private final boolean isReadonly;

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
    private Callable<CompletableFuture<Void>> externalCommitStep;

    /**
     * Array of keys to lock for this transaction.
     */
    @Getter
    private final String[] keysToLock;

    /**
     * Constructor.
     *
     * @param store   Underlying metadata store.
     * @param isReadonly Whether transaction is read only or not.
     * @param version Version number of the transactions.
     * @param keysToLock Array of keys to lock for this transaction.
     */
    public MetadataTransaction(ChunkMetadataStore store, boolean isReadonly, long version, String... keysToLock) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.version = version;
        this.keysToLock = Preconditions.checkNotNull(keysToLock, "keys");
        this.isReadonly = isReadonly;
        Preconditions.checkArgument(keysToLock.length > 0, "At least one key must be locked.");
        data = new ConcurrentHashMap<>();
    }

    /**
     * Retrieves the metadata for given key.
     *
     * @param key key to use to retrieve metadata.
     * @return Metadata for given key. Null if key was not found.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public CompletableFuture<StorageMetadata> get(String key) {
        return store.get(this, key);
    }

    /**
     * Updates existing metadata.
     *
     * @param metadata metadata record.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public void update(StorageMetadata metadata) {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        store.update(this, metadata);
    }

    /**
     * Creates a new metadata record.
     *
     * @param metadata metadata record.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public void create(StorageMetadata metadata) {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        store.create(this, metadata);
    }

    /**
     * Marks given record as pinned.
     *
     * @param metadata metadata record.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public void markPinned(StorageMetadata metadata) {
        store.markPinned(this, metadata);
    }

    /**
     * Deletes a metadata record given the key.
     *
     * @param key key to use to retrieve metadata.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public void delete(String key) {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        store.delete(this, key);
    }

    /**
     * Commits the transaction.
     *
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public CompletableFuture<Void> commit() {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        return store.commit(this);
    }

    /**
     * Commits the transaction.
     *
     * @param lazyWrite true if data can be written lazily.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public CompletableFuture<Void> commit(boolean lazyWrite) {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        return store.commit(this, lazyWrite);
    }


    /**
     * Commits the transaction.
     *
     * @param lazyWrite      true if data can be written lazily.
     * @param skipStoreCheck true if data is not to be reloaded from store.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public CompletableFuture<Void> commit(boolean lazyWrite, boolean skipStoreCheck) {
        Preconditions.checkState(!isReadonly, "Attempt to modify in readonly transaction");
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        return store.commit(this, lazyWrite, skipStoreCheck);
    }

    /**
     * Marks transaction as committed.
     */
    public void setCommitted() {
        isCommitted = true;
    }

    /**
     * Aborts the transaction.
     *
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link StorageMetadataException} Exception related to storage metadata operations.
     */
    public CompletableFuture<Void> abort() {
        Preconditions.checkState(!isCommitted, "Transaction is already committed");
        Preconditions.checkState(!isAborted, "Transaction is already aborted");
        isAborted = true;
        return store.abort(this);
    }

    /**
     * {@link AutoCloseable#close()} implementation.
     *
     */
    @Override
    public void close() {
        if (!isCommitted || isAborted) {
            store.abort(this);
        }
        store.closeTransaction(this);
    }
}

