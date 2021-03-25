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
package io.pravega.controller.store;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.store.stream.Cache;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.util.RetryHelper;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionDropped;
import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionFailed;

/**
 * Helper class for all table related queries to segment store. This class invokes appropriate wire command calls into
 * SegmentHelper for all table related apis and then translates the failure responses to Store specific exceptions.
 */
@Slf4j
public class PravegaTablesStoreHelper {
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    private final AtomicReference<String> authToken;
    private final GrpcAuthHelper authHelper;
    private final int numOfRetries;
    
    @lombok.Data
    @EqualsAndHashCode(exclude = {"fromBytesFunc"})
    private class TableCacheKey<T> implements Cache.CacheKey {
        private final String table;
        private final String key;

        private final Function<byte[], T> fromBytesFunc;
    }

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, GrpcAuthHelper authHelper, ScheduledExecutorService executor) {
        this(segmentHelper, authHelper, executor, NUM_OF_RETRIES);
    }

    @VisibleForTesting
    PravegaTablesStoreHelper(SegmentHelper segmentHelper, GrpcAuthHelper authHelper, ScheduledExecutorService executor, int numOfRetries) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;

        cache = new Cache();
        this.authHelper = authHelper;
        this.authToken = new AtomicReference<>(authHelper.retrieveMasterToken());
        this.numOfRetries = numOfRetries;
    }

    /**
     * Api to read cached value for the specified key from the requested table.
     * @param table name of table
     * @param key key to query
     * @param fromBytes deserialization function.
     * @param <T> Type of object to deserialize the response into.
     * @return Returns a completableFuture which when completed will have the deserialized value with its store key version.
     */
    public <T> CompletableFuture<VersionedMetadata<T>> getCachedData(String table, String key, Function<byte[], T> fromBytes) {
        TableCacheKey<T> cacheKey = new TableCacheKey<>(table, key, fromBytes);
        VersionedMetadata<?> cached = cache.getCachedData(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(getVersionedMetadata(cached));
        } else {
            return getEntry(table, key, fromBytes)
                    .thenApply(v -> {
                        VersionedMetadata<T> record = new VersionedMetadata<>(v.getObject(), v.getVersion());
                        cache.put(cacheKey, record);
                        return record;
                    });
        }
    }

    @SuppressWarnings("unchecked")
    private <T> VersionedMetadata<T> getVersionedMetadata(VersionedMetadata v) {
        // Since cache is untyped and holds all types of deserialized objects, we typecast it to the requested object type
        // based on the type in caller's supplied Deserialization function.
        return new VersionedMetadata<>((T) v.getObject(), v.getVersion());
    }

    /**
     * Method to invalidate cached value in the cache for the specified table.
     * @param table table name
     * @param key key to invalidate
     */
    public void invalidateCache(String table, String key) {
        cache.invalidateCache(new TableCacheKey<>(table, key, x -> null));
    }

    /**
     * Method to create a new Table. If the table already exists, segment helper responds with success.
     * @param tableName table name
     * @return CompletableFuture which when completed will indicate successful creation of table.
     */
    public CompletableFuture<Void> createTable(String tableName) {
        log.debug("create table called for table: {}", tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(tableName, authToken.get(), RequestTag.NON_EXISTENT_ID, false),
                () -> String.format("create table: %s", tableName)))
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        log.warn("create table {} threw exception", tableName, e);
                    } else {
                        log.debug("table {} created successfully", tableName);
                    }
                }, executor);
    }

    /**
     * Method to delete a table. The callers can supply a `mustBeEmpty` flag and the table is deleted only if it is empty.
     * Note: the mustBeEmpty flag is passed to segment store via segment helper and it is responsibility of segment store
     * table implementation to delete a table only if it is empty.
     * @param tableName tableName
     * @param mustBeEmpty flag to indicate to table store to delete table only if it is empty.
     * @return CompletableFuture which when completed will indicate that the table was deleted successfully.
     * If mustBeEmpty is set to true and the table is non-empty then the future is failed with StoreException.DataNotEmptyException
     */
    public CompletableFuture<Void> deleteTable(String tableName, boolean mustBeEmpty) {
        log.debug("delete table called for table: {}", tableName);
        return expectingDataNotFound(withRetries(() -> segmentHelper.deleteTableSegment(
                tableName, mustBeEmpty, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("delete table: %s", tableName)), null)
                .thenAcceptAsync(v -> log.debug("table {} deleted successfully", tableName), executor);
    }

    /**
     * Method to add new entry to a table.
     * @param tableName table name
     * @param key key to add
     * @param value value to add
     * @return CompletableFuture which when completed will have the version of the key returned by segment store.
     * If the key already exists, it will throw StoreException.DataExistsException.
     */
    public CompletableFuture<Version> addNewEntry(String tableName, String key, @NonNull byte[] value) {
        log.trace("addNewEntry called for : {} key : {}", tableName, key);

        List<TableSegmentEntry> entries = Collections.singletonList(
                TableSegmentEntry.notExists(key.getBytes(Charsets.UTF_8), value));
        Supplier<String> errorMessage = () -> String.format("addNewEntry: key: %s table: %s", key, tableName);
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                errorMessage, true)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StoreException.WriteConflictException) {
                        throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                    } else {
                        log.debug("add new entry {} to {} threw exception {} {}", key, tableName, unwrap.getClass(), unwrap.getMessage());
                        throw new CompletionException(e);
                    }
                })
                .thenApplyAsync(x -> {
                    TableSegmentKeyVersion first = x.get(0);
                    log.trace("entry for key {} added to table {} with version {}", key, tableName, first.getSegmentVersion());
                    return (Version) new Version.LongVersion(first.getSegmentVersion());
                }, executor)
                .whenComplete((r, ex) -> releaseEntries(entries));
    }

    /**
     * Method to add new entry to table if it does not exist.
     * @param tableName tableName
     * @param key Key to add
     * @param value value to add
     * @return CompletableFuture which when completed will have added entry to the table if it did not exist.
     */
    public CompletableFuture<Version> addNewEntryIfAbsent(String tableName, String key, @NonNull byte[] value) {
        // if entry exists, we will get write conflict in attempting to create it again.
        return expectingDataExists(addNewEntry(tableName, key, value), null);
    }

    /**
     * Method to add a batch of entries if absent. Table implementation on segment store guarantees that either all or none of
     * the entries are added.
     * If segment store responds with success, then it is guaranteed that all entries are added to the store.
     * However, it is important to note that the segment store could respond with Data Exists even if one of the entries exists.
     * In such case, this method will ignore data exist and respond with success for the entire batch. It does not verify
     * if all entries existed or one of the entries existed.
     * Callers should use this only if they are guaranteed to never create the requested entries outside of the requested batch.
     *
     * @param tableName table name
     * @param toAdd map of keys and values to add.
     * @return CompletableFuture which when completed successfully will indicate that all entries have been added successfully.
     */
    public CompletableFuture<Void> addNewEntriesIfAbsent(String tableName, Map<String, byte[]> toAdd) {
        List<TableSegmentEntry> entries = toAdd.entrySet().stream().map(x ->
                TableSegmentEntry.notExists(x.getKey().getBytes(Charsets.UTF_8), x.getValue()))
                                               .collect(Collectors.toList());
        Supplier<String> errorMessage = () -> String.format("addNewEntriesIfAbsent: table: %s", tableName);
        return expectingDataExists(withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(),
                RequestTag.NON_EXISTENT_ID), errorMessage)
                .handle((r, e) -> {
                    releaseEntries(entries);
                    if (e != null) {
                        Throwable unwrap = Exceptions.unwrap(e);
                        if (unwrap instanceof StoreException.WriteConflictException) {
                            throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage.get());
                        } else {
                            log.debug("add new entries to {} threw exception {} {}", tableName, unwrap.getClass(), unwrap.getMessage());
                            throw new CompletionException(e);
                        }
                    } else {
                        log.trace("entries added to table {}", tableName);
                        return null;
                    }
                }), null);
    }

    /**
     * Method to update a single entry.
     * @param tableName tablename
     * @param key key
     * @param value value
     * @param ver previous key version
     * @return CompletableFuture which when completed will indicate that the value is updated in the table.
     */
    public CompletableFuture<Version> updateEntry(String tableName, String key, byte[] value, Version ver) {
        long version = ver.asLongVersion().getLongValue();
        log.trace("updateEntry entry called for : {} key : {} version {}", tableName, key, version);

        List<TableSegmentEntry> entries = Collections.singletonList(
                TableSegmentEntry.versioned(key.getBytes(Charsets.UTF_8), value, version));
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("updateEntry: key: %s table: %s", key, tableName), true)
                .thenApplyAsync(x -> {
                    TableSegmentKeyVersion first = x.get(0);
                    log.trace("entry for key {} updated to table {} with new version {}", key, tableName, first.getSegmentVersion());
                    return (Version) new Version.LongVersion(first.getSegmentVersion());
                }, executor)
                .whenComplete((r, ex) -> releaseEntries(entries));
    }

    /**
     * Method to retrieve the value for a given key from a table. This method takes a deserialization function and deserializes
     * the received byte[] using the supplied function.
     * @param tableName tableName
     * @param key key
     * @param fromBytes deserialization function
     * @param <T> Type of deserialized object
     * @return CompletableFuture which when completed will have the versionedMetadata retrieved from the store.
     */
    public <T> CompletableFuture<VersionedMetadata<T>> getEntry(String tableName, String key, Function<byte[], T> fromBytes) {
        log.trace("get entry called for : {} key : {}", tableName, key);
        List<TableSegmentKey> keys = Collections.singletonList(TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8)));
        CompletableFuture<VersionedMetadata<T>> result = new CompletableFuture<>();
        String message = "get entry: key: %s table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, key, tableName))
                .thenApplyAsync(x -> {
                    try {
                        TableSegmentEntry first = x.get(0);
                        if (first.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, String.format(message, key, tableName));
                        } else {
                            log.trace("returning entry for : {} key : {} with version {}", tableName, key,
                                    first.getKey().getVersion().getSegmentVersion());

                            T deserialized = fromBytes.apply(getArray(first.getValue()));

                            return new VersionedMetadata<>(deserialized, new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                        }
                    } finally {
                        releaseEntries(x);
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
                    releaseKeys(keys);
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                }, executor);
        return result;
    }

    /**
     * Method to retrieve the value for a given key from a table. This method takes a deserialization function and deserializes
     * the received byte[] using the supplied function.
     * @param tableName tableName
     * @param keys keys to read
     * @param fromBytes deserialization function
     * @param nonExistent entry to populate for non existent keys
     * @param <T> Type of deserialized object
     * @return CompletableFuture which when completed will have the versionedMetadata retrieved from the store.
     */
    public <T> CompletableFuture<List<VersionedMetadata<T>>> getEntries(String tableName, List<String> keys, 
                                                                        Function<byte[], T> fromBytes, VersionedMetadata<T> nonExistent) {
        log.trace("get entries called for : {} keys : {}", tableName, keys);
        List<TableSegmentKey> tableKeys = keys.stream().map(key -> TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8)))
                                              .collect(Collectors.toList());
        CompletableFuture<List<VersionedMetadata<T>>> result = new CompletableFuture<>();

        String message = "get entry: key: %s table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, tableKeys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, keys, tableName))
                .thenApplyAsync(entries -> {
                    try {
                        return entries.stream().map(entry -> {
                            if (entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                                return nonExistent;
                            } else {
                                return new VersionedMetadata<>(fromBytes.apply(getArray(entry.getValue())),
                                        new Version.LongVersion(entry.getKey().getVersion().getSegmentVersion()));
                            }
                        }).collect(Collectors.toList());
                    } finally {
                        releaseEntries(entries);
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
                    releaseKeys(tableKeys);
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                }, executor);
        
        return result;
    }

    /**
     * Method to remove entry from the store.
     * @param tableName tableName
     * @param key key
     * @return CompletableFuture which when completed will indicate successful deletion of entry from the table.
     * It ignores DataNotFound exception.
     */
    public CompletableFuture<Void> removeEntry(String tableName, String key) {
        return removeEntry(tableName, key, null);
    }

    /**
     * Method to remove entry from the store. 
     * @param tableName tableName
     * @param key key
     * @param ver version for conditional removal
     * @return CompletableFuture which when completed will indicate successful deletion of entry from the table. 
     * It ignores DataNotFound exception. 
     */
    public CompletableFuture<Void> removeEntry(String tableName, String key, Version ver) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        TableSegmentKey tableKey = ver == null
                ? TableSegmentKey.unversioned(key.getBytes(Charsets.UTF_8))
                : TableSegmentKey.versioned(key.getBytes(Charsets.UTF_8), ver.asLongVersion().getLongValue());

        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, Collections.singletonList(tableKey), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entry: key: %s table: %s", key, tableName)), null)
                .thenAcceptAsync(v -> log.trace("entry for key {} removed from table {}", key, tableName), executor)
                .whenComplete((r, ex) -> releaseKeys(Collections.singleton(tableKey)));
    }

    /**
     * Removes a batch of entries from the table store. Ignores data not found exception and treats it as success.
     * If table store throws dataNotFound for a subset of entries, there is no way for this method to disambiguate.
     * So it is the responsibility of the caller to use this api if they are guaranteed to always attempt to
     * remove same batch entries.
     * @param tableName table name
     * @param keys keys to delete
     * @return CompletableFuture which when completed will have entries removed from the table.
     */
    public CompletableFuture<Void> removeEntries(String tableName, Collection<String> keys) {
        log.trace("remove entry called for : {} keys : {}", tableName, keys);

        List<TableSegmentKey> listOfKeys = keys.stream()
                                               .map(x -> TableSegmentKey.unversioned(x.getBytes(Charsets.UTF_8)))
                                               .collect(Collectors.toList());
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, listOfKeys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entries: keys: %s table: %s", keys.toString(), tableName)), null)
                .thenAcceptAsync(v -> log.trace("entry for keys {} removed from table {}", keys, tableName), executor)
                .whenComplete((r, ex) -> releaseKeys(listOfKeys));
    }

    /**
     * Method to get paginated list of keys with a continuation token.
     * @param tableName tableName
     * @param continuationToken previous continuationToken
     * @param limit limit on number of keys to retrieve
     * @return CompletableFuture which when completed will have a list of keys of size less than or equal to limit and
     * a new ContinutionToken.
     */
    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String tableName, ByteBuf continuationToken, int limit) {
        log.trace("get keys paginated called for : {}", tableName);
        return withRetries(() ->
                        segmentHelper.readTableKeys(tableName, limit, IteratorStateImpl.fromBytes(continuationToken), authToken.get(),
                                RequestTag.NON_EXISTENT_ID),
                () -> String.format("get keys paginated for table: %s", tableName))
                .thenApplyAsync(result -> {
                    try {
                        List<String> items = result.getItems().stream().map(x -> new String(getArray(x.getKey()), Charsets.UTF_8))
                                .collect(Collectors.toList());
                        log.trace("get keys paginated on table {} returned items {}", tableName, items);
                        // if the returned token and result are empty, return the incoming token so that
                        // callers can resume from that token.
                        return new AbstractMap.SimpleEntry<>(getNextToken(continuationToken, result), items);
                    } finally {
                        releaseKeys(result.getItems());
                    }
                }, executor);
    }

    /**
     * Method to get paginated list of entries with a continuation token.
     * @param tableName tableName
     * @param continuationToken previous continuationToken
     * @param limit limit on number of entries to retrieve
     * @param fromBytes function to deserialize byte array into object of type T
     * @param <T> type of deserialized entry values
     * @return CompletableFuture which when completed will have a list of entries of size less than or equal to limit and
     * a new ContinutionToken.
     */
    public <T> CompletableFuture<Map.Entry<ByteBuf, List<Map.Entry<String, VersionedMetadata<T>>>>> getEntriesPaginated(
            String tableName, ByteBuf continuationToken, int limit,
            Function<byte[], T> fromBytes) {
        log.trace("get entries paginated called for : {}", tableName);
        return withRetries(() -> segmentHelper.readTableEntries(tableName, limit,
                IteratorStateImpl.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s", tableName))
                .thenApplyAsync(result -> {
                    try {
                        List<Map.Entry<String, VersionedMetadata<T>>> items = result.getItems().stream().map(x -> {
                            String key = new String(getArray(x.getKey().getKey()), Charsets.UTF_8);
                            T deserialized = fromBytes.apply(getArray(x.getValue()));
                            VersionedMetadata<T> value = new VersionedMetadata<>(deserialized, new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                            return new AbstractMap.SimpleEntry<>(key, value);
                        }).collect(Collectors.toList());
                        log.trace("get keys paginated on table {} returned number of items {}", tableName, items.size());
                        // if the returned token and result are empty, return the incoming token so that 
                        // callers can resume from that token. 
                        return new AbstractMap.SimpleEntry<>(getNextToken(continuationToken, result), items);
                    } finally {
                        releaseEntries(result.getItems());
                    }
                }, executor);
    }

    private ByteBuf getNextToken(ByteBuf continuationToken, IteratorItem<?> result) {
        return result.getItems().isEmpty() && result.getState().isEmpty() ?
                continuationToken : Unpooled.wrappedBuffer(result.getState().toBytes());
    }

    /**
     * Method to retrieve all keys in the table. It returns an asyncIterator which can be used to iterate over the returned keys.
     * @param tableName table name
     * @return AsyncIterator that can be used to iterate over keys in the table.
     */
    public AsyncIterator<String> getAllKeys(String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(tableName, token, 1000)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorStateImpl.EMPTY.getToken());
    }

    /**
     * Method to retrieve all entries in the table. It returns an asyncIterator which can be used to iterate over the returned entries.
     * @param tableName table name
     * @param fromBytes deserialization function to deserialize returned value.
     * @param <T> Type of value
     * @return AsyncIterator that can be used to iterate over keys in the table.
     */
    public <T> AsyncIterator<Map.Entry<String, VersionedMetadata<T>>> getAllEntries(String tableName, Function<byte[], T> fromBytes) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(tableName, token, 1000, fromBytes)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorStateImpl.EMPTY.getToken());
    }

    public <T> CompletableFuture<T> expectingDataNotFound(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, toReturn);
    }

    <T> CompletableFuture<T> expectingDataExists(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, toReturn);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future,
                                                                   Supplier<String> errorMessageSupplier,
                                                                   boolean throwOriginalOnCFE) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                        toThrow = throwOriginalOnCFE ? wcfe :
                                StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case UnknownHost:
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case PreconditionFailed:
                        toThrow = StoreException.create(StoreException.Type.ILLEGAL_STATE, wcfe, errorMessage);
                        break;
                    case AuthFailed:
                        authToken.set(authHelper.retrieveMasterToken());
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableSegmentNotEmpty:
                        toThrow = StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, wcfe, errorMessage);
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreException.create(StoreException.Type.WRITE_CONFLICT, wcfe, errorMessage);
                        break;
                    default:
                        toThrow = StoreException.create(StoreException.Type.UNKNOWN, wcfe, errorMessage);
                }
            } else if (cause instanceof HostStoreException) {
                log.warn("Host Store exception {}", cause.getMessage());
                toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, cause, errorMessage);
            } else {
                log.warn("exception of unknown type thrown {} ", errorMessage, cause);
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    /*
     * We don't want to do indefinite retries because for controller's graceful shutting down, it waits on grpc service to
     * be terminated which in turn waits on all outstanding grpc calls to complete. And the store may stall the calls if
     * there is indefinite retries. Restricting it to 12 retries gives us ~60 seconds worth of wait on the upper side.
     * Also, note that the call can fail because hostContainerMap has not been updated or it can fail because it cannot
     * talk to segment store. Both these are translated to ConnectionErrors and are retried. All other exceptions
     * are thrown back
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage) {
        return withRetries(futureSupplier, errorMessage, false);
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage,
                                         boolean throwOriginalOnCfe) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage, throwOriginalOnCfe),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException, numOfRetries, executor)
                .exceptionally(e -> {
                    Throwable t = Exceptions.unwrap(e);
                    if (t instanceof RetriesExhaustedException) {
                        throw new CompletionException(t.getCause());
                    } else {
                        Throwable unwrap = Exceptions.unwrap(e);
                        if (unwrap instanceof WireCommandFailedException &&
                                (((WireCommandFailedException) unwrap).getReason().equals(ConnectionDropped) ||
                                        ((WireCommandFailedException) unwrap).getReason().equals(ConnectionFailed))) {
                            throw new CompletionException(StoreException.create(StoreException.Type.CONNECTION_ERROR,
                                    errorMessage.get()));
                        } else {
                            throw new CompletionException(unwrap);
                        }
                    }
                });
    }

    private byte[] getArray(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return bytes;
    }

    private void releaseKeys(Collection<TableSegmentKey> keys) {
        for (TableSegmentKey k : keys) {
            ReferenceCountUtil.safeRelease(k.getKey());
        }
    }

    private void releaseEntries(Collection<TableSegmentEntry> entries) {
        for (TableSegmentEntry e : entries) {
            ReferenceCountUtil.safeRelease(e.getKey().getKey());
            ReferenceCountUtil.safeRelease(e.getValue());
        }
    }
}
