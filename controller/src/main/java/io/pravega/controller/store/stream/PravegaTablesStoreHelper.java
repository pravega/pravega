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
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.util.RetryHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class PravegaTablesStoreHelper {
    private static final int NUM_OF_TRIES = Integer.MAX_VALUE;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final Cache<CacheKey> cache;
    private final AtomicReference<String> authToken;
    @lombok.Data
    private class CacheKey {
        private final String scope;
        private final String table;
        private final String key;
    }
    
    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, ScheduledExecutorService executor) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;

        cache = new Cache<>(entryKey -> {
            // Since there are be multiple tables, we will cache `table+key` in our cache
            return getEntry(entryKey.getScope(), entryKey.getTable(), entryKey.getKey());
        });
        this.authToken = new AtomicReference<>(segmentHelper.retrieveMasterToken());
    }

    CompletionStage<Data> getCachedData(String scope, String table, String key) {
        return cache.getCachedData(new CacheKey(scope, table, key));
    }

    void invalidateCache(String scope, String table, String key) {
        cache.invalidateCache(new CacheKey(scope, table, key));
    }

    public CompletableFuture<Void> createTable(String scope, String tableName) {
        log.debug("create table called for table: {}/{}", scope, tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(scope, tableName, authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("create table: %s/%s", scope, tableName)))
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        log.warn("create table {}/{} threw exception", scope, tableName, e);
                    } else {
                        log.debug("table {}/{} created successfully", scope, tableName);
                    }
                }, executor);
    }
    
    public CompletableFuture<Void> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        log.debug("delete table called for table: {}/{}", scope, tableName);
        return withRetries(() -> segmentHelper.deleteTableSegment(scope, tableName, mustBeEmpty, authToken.get(), RequestTag.NON_EXISTENT_ID),
                        String.format("delete table: %s/%s", scope, tableName))
                                   .thenAcceptAsync(v -> log.debug("table {}/{} deleted successfully", scope, tableName), executor);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, @NonNull byte[] value) {
        log.debug("addNewEntry called for : {}/{} key : {}", scope, tableName, key);

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), KeyVersion.NOT_EXISTS), value));
        String errorMessage = String.format("addNewEntry: key: %s table: %s/%s", key, scope, tableName);
        return withRetries(() -> segmentHelper.updateTableEntries(scope, tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                errorMessage)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StoreException.WriteConflictException) {
                        throw StoreException.create(StoreException.Type.DATA_EXISTS, errorMessage);
                    } else {
                        log.debug("add new entry {} to {}/{} threw exception {} {}", key, scope, tableName, unwrap.getClass(), unwrap.getMessage());
                        throw new CompletionException(e);
                    }
                })
                .thenApplyAsync(x -> {
                    KeyVersion first = x.get(0);
                    log.debug("entry for key {} added to table {}/{} with version {}", key, scope, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
    }

    public CompletableFuture<Version> addNewEntryIfAbsent(String scope, String tableName, String key, @NonNull byte[] value) {
        // if entry exists, we will get write conflict in attempting to create it again. 
        return Futures.exceptionallyExpecting(addNewEntry(scope, tableName, key, value),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, null);
    }

    public CompletableFuture<Version> updateEntry(String scope, String tableName, String key, Data value) {
        log.debug("updateEntry entry called for : {}/{} key : {} version {}", scope, tableName, key, value.getVersion().asLongVersion().getLongValue());

        KeyVersionImpl version = value.getVersion() == null ? null :
                new KeyVersionImpl(value.getVersion().asLongVersion().getLongValue());

        List<TableEntry<byte[], byte[]>> entries = Collections.singletonList(
                new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), version), value.getData()));
        return withRetries(() -> segmentHelper.updateTableEntries(scope, tableName, entries, authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("updateEntry: key: %s table: %s/%s", key, scope, tableName))
                .thenApplyAsync(x -> {
                    KeyVersion first = x.get(0);
                    log.debug("entry for key {} updated to table {}/{} with new version {}", key, scope, tableName, first.getSegmentVersion());
                    return new Version.LongVersion(first.getSegmentVersion());
                }, executor);
    }

    public CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        log.debug("get entry called for : {}/{} key : {}", scope, tableName, key);
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        CompletableFuture<Data> result = new CompletableFuture<>();
        withRetries(() -> segmentHelper.readTable(scope, tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("get entry: key: %s table: %s/%s", key, scope, tableName))
                .thenApplyAsync(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    log.debug("returning entry for : {}/{} key : {} with version {}", scope, tableName, key, 
                            first.getKey().getVersion().getSegmentVersion());

                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
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

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        log.debug("remove entry called for : {}/{} key : {}", scope, tableName, key);

        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        return withRetries(() -> segmentHelper.removeTableKeys(scope, tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("remove entry: key: %s table: %s/%s", key, scope, tableName))
                .thenAcceptAsync(v -> log.debug("entry for key {} removed from table {}/{}", key, scope, tableName), executor);
    }

    public CompletableFuture<Void> removeEntries(String scope, String tableName, List<String> keys) {
        log.debug("remove entry called for : {}/{} keys : {}", scope, tableName, keys);

        List<TableKey<byte[]>> listOfKeys = keys.stream().map(x -> new TableKeyImpl<>(x.getBytes(Charsets.UTF_8), null)).collect(Collectors.toList());
        return withRetries(() -> segmentHelper.removeTableKeys(scope, tableName, listOfKeys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("remove entries: keys: %s table: %s/%s", keys.toString(), scope, tableName))
                .thenAcceptAsync(v -> log.debug("entry for keys {} removed from table {}/{}", keys, scope, tableName), executor);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String scope, String tableName, ByteBuf continuationToken, int limit) {
        log.debug("get keys paginated called for : {}/{}", scope, tableName);

        return withRetries(() -> 
                segmentHelper.readTableKeys(scope, tableName, limit, IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                        String.format("get keys paginated for table: %s/%s", scope, tableName))
                             .thenApplyAsync(result -> {
                                 List<String> items = result.getItems().stream().map(x -> new String(x.getKey(), Charsets.UTF_8))
                                                            .collect(Collectors.toList());
                                 log.debug("get keys paginated on table {}/{} returned items {}", scope, tableName, items);
                                 return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                             }, executor);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<Pair<String, Data>>>> getEntriesPaginated(String scope, String tableName, 
                                                                                               ByteBuf continuationToken, int limit) {
        log.debug("get entries paginated called for : {}/{}", scope, tableName);

        return withRetries(() -> segmentHelper.readTableEntries(scope, tableName, limit,
                IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                String.format("get entries paginated for table: %s/%s", scope, tableName))
                .thenApplyAsync(result -> {
                    List<Pair<String, Data>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey(), Charsets.UTF_8);
                        Data value = new Data(x.getValue(), new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                        return new ImmutablePair<>(key, value);
                    }).collect(Collectors.toList());
                    log.debug("get keys paginated on table {}/{} returned number of items {}", scope, tableName, items.size());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(scope, tableName, token, 1000)
                .thenApplyAsync(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue()), executor),
                IteratorState.EMPTY.toBytes());
    }

    public AsyncIterator<Pair<String, Data>> getAllEntries(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(scope, tableName, token, 100)
                .thenApplyAsync(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue()), executor),
                IteratorState.EMPTY.toBytes());
    }

    private <T> Supplier<CompletableFuture<T>> exceptionallCallback(Supplier<CompletableFuture<T>> future, String errorMessage) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
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
                        authToken.set(segmentHelper.retrieveMasterToken());
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
                log.warn("error {} {}", errorMessage, cause.getClass());
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, String errorMessage) {
        return RetryHelper.withRetriesAsync(exceptionallCallback(futureSupplier, errorMessage), 
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException, NUM_OF_TRIES, executor);
    }
}
