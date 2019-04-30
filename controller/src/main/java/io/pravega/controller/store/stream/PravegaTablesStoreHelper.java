/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.netty.buffer.ByteBuf;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.util.RetryHelper;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

@Slf4j
/**
 * Helper class for all table related queries to segment store. This class invokes appropriate wire command calls into
 * SegmentHelper for all table related apis and then translates the failure responses to Store specific exceptions.   
 */
public class PravegaTablesStoreHelper {
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final Cache cache;
    private final AtomicReference<String> authToken;
    private final AuthHelper authHelper;
    
    @lombok.Data
    @EqualsAndHashCode(exclude = {"fromBytesFunc"})
    private class TableCacheKey<T> implements Cache.CacheKey {
        private final String table;
        private final String key;

        private final Function<byte[], T> fromBytesFunc;
    }

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, AuthHelper authHelper, ScheduledExecutorService executor) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;

        cache = new Cache(x -> {
            TableCacheKey<?> entryKey = (TableCacheKey<?>) x;

            // Since there are be multiple tables, we will cache `table+key` in our cache
            return getEntry(entryKey.getTable(), entryKey.getKey(), entryKey.fromBytesFunc)
                                   .thenApply(v -> new VersionedMetadata<>(v.getObject(), v.getVersion()));
        });
        this.authHelper = authHelper;
        this.authToken = new AtomicReference<>(authHelper.retrieveMasterToken());
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
        return cache.getCachedData(new TableCacheKey<>(table, key, fromBytes))
                    .thenApply(this::getVersionedMetadata);
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

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(tableName, authToken.get(), RequestTag.NON_EXISTENT_ID),
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

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), KeyVersion.NOT_EXISTS), value));
        Supplier<String> errorMessage = () -> String.format("addNewEntry: key: %s table: %s", key, tableName);
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                errorMessage)
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
                    KeyVersion first = x.get(0);
                    log.trace("entry for key {} added to table {} with version {}", key, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
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
        List<TableEntry<byte[], byte[]>> entries = toAdd.entrySet().stream().map(x ->
                new TableEntryImpl<>(new TableKeyImpl<>(x.getKey().getBytes(Charsets.UTF_8), KeyVersion.NOT_EXISTS), x.getValue()))
                                                        .collect(Collectors.toList());
        Supplier<String> errorMessage = () -> String.format("addNewEntriesIfAbsent: table: %s", tableName);
        return expectingDataExists(withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), 
                        RequestTag.NON_EXISTENT_ID), errorMessage)
                .handle((r, e) -> {
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
        log.trace("updateEntry entry called for : {} key : {} version {}", tableName, key, ver.asLongVersion().getLongValue());

        KeyVersionImpl version = new KeyVersionImpl(ver.asLongVersion().getLongValue());

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), version), value));
        return withRetries(() -> segmentHelper.updateTableEntries(tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("updateEntry: key: %s table: %s", key, tableName))
                .thenApplyAsync(x -> {
                    KeyVersion first = x.get(0);
                    log.trace("entry for key {} updated to table {} with new version {}", key, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
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
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        CompletableFuture<VersionedMetadata<T>> result = new CompletableFuture<>();
        String message = "get entry: key: %s table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, key, tableName))
                .thenApplyAsync(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    if (first.getKey().getVersion().equals(KeyVersion.NOT_EXISTS)) {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, String.format(message, key, tableName));   
                    } else {
                        log.trace("returning entry for : {} key : {} with version {}", tableName, key,
                                first.getKey().getVersion().getSegmentVersion());

                        T deserialized = fromBytes.apply(first.getValue());

                        return new VersionedMetadata<>(deserialized, new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                    }
                }, executor)
                .whenCompleteAsync((r, e) -> {
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
        log.trace("remove entry called for : {} key : {}", tableName, key);

        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID), 
                () -> String.format("remove entry: key: %s table: %s", key, tableName)), null)
                .thenAcceptAsync(v -> log.trace("entry for key {} removed from table {}", key, tableName), executor);
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

        List<TableKey<byte[]>> listOfKeys = keys.stream().map(x -> new TableKeyImpl<>(x.getBytes(Charsets.UTF_8), null)).collect(Collectors.toList());
        return expectingDataNotFound(withRetries(() -> segmentHelper.removeTableKeys(
                tableName, listOfKeys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entries: keys: %s table: %s", keys.toString(), tableName)), null)
                .thenAcceptAsync(v -> log.trace("entry for keys {} removed from table {}", keys, tableName), executor);
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
                segmentHelper.readTableKeys(tableName, limit, IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                        () -> String.format("get keys paginated for table: %s", tableName))
                             .thenApplyAsync(result -> {
                                 List<String> items = result.getItems().stream().map(x -> new String(x.getKey(), Charsets.UTF_8))
                                                            .collect(Collectors.toList());
                                 log.trace("get keys paginated on table {} returned items {}", tableName, items);
                                 return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
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
                IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s", tableName))
                .thenApplyAsync(result -> {
                    List<Map.Entry<String, VersionedMetadata<T>>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey(), Charsets.UTF_8);
                        T deserialized = fromBytes.apply(x.getValue());
                        VersionedMetadata<T> value = new VersionedMetadata<>(deserialized, new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                        return new AbstractMap.SimpleEntry<>(key, value);
                    }).collect(Collectors.toList());
                    log.trace("get keys paginated on table {} returned number of items {}", tableName, items.size());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    /**
     * Method to retrieve a collection of entries bounded by the specified limit size that satisfy the supplied filter. 
     * This function makes calls into segment store and includes entries that satisfy the supplied
     * predicate. It makes repeated paginated calls into segment store until it has either collected deseried number
     * of entries or it has exhausted all entries in the store. 
     * @param table table
     * @param fromStringKey function to deserialize key from String. 
     * @param fromBytesValue function to deserialize value from byte array
     * @param filter filer predicate which takes key and value and returns true or false.  
     * @param limit maximum number of entries to retrieve 
     * @param <K> Type of Key
     * @param <V> Type of Value
     * @return CompletableFuture which when completed will have a map of keys and values of size bounded by supplied limit 
     */
    public <K, V> CompletableFuture<Map<K, V>> getEntriesWithFilter(
            String table, Function<String, K> fromStringKey,
            Function<byte[], V> fromBytesValue, BiFunction<K, V, Boolean> filter, int limit) {
        Map<K, V> result = new ConcurrentHashMap<>();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        AtomicReference<ByteBuf> token = new AtomicReference<>(IteratorState.EMPTY.toBytes());

        return Futures.exceptionallyExpecting(
                Futures.loop(canContinue::get,
                        () -> getEntriesPaginated(table, token.get(), limit, fromBytesValue)
                                .thenAccept(v -> {
                                    // we exit if we have either received `limit` number of entries
                                    List<Map.Entry<String, VersionedMetadata<V>>> pair = v.getValue();
                                    for (Map.Entry<String, VersionedMetadata<V>> val : pair) {
                                        K key = fromStringKey.apply(val.getKey());
                                        V value = val.getValue().getObject();
                                        if (filter.apply(key, value)) {
                                            result.put(key, value);
                                            if (result.size() == limit) {
                                                break;
                                            }
                                        }
                                    }
                                    // if we get less than the requested number, then we will exit the loop. 
                                    // otherwise if we have collected all the desired results
                                    canContinue.set(!(v.getValue().size() < limit || result.size() >= limit));
                                    token.get().release();
                                    if (canContinue.get()) {
                                        // set next continuation token
                                        token.set(v.getKey());
                                    }
                                }), executor)
                       .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap());
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
                IteratorState.EMPTY.toBytes());
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
                IteratorState.EMPTY.toBytes());
    }

    <T> CompletableFuture<T> expectingDataNotFound(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, toReturn);
    }

    <T> CompletableFuture<T> expectingDataExists(CompletableFuture<T> future, T toReturn) {
        return Futures.exceptionallyExpecting(future, e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, toReturn);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future, Supplier<String> errorMessageSupplier) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
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
     * We dont want to do indefinite retries because for controller's graceful shutting down, it waits on grpc service to
     * be terminated which in turn waits on all outstanding grpc calls to complete. And the store may stall the calls if
     * there is indefinite retries. Restricting it to 12 retries gives us ~60 seconds worth of wait on the upper side.
     * Also, note that the call can fail because hostContainerMap has not been updated or it can fail because it cannot
     * talk to segment store. Both these are translated to ConnectionErrors and are retried. All other exceptions
     * are thrown back
     */
    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage),
                e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    return unwrap instanceof StoreException.StoreConnectionException;
                }, NUM_OF_RETRIES, executor);
    }
}
