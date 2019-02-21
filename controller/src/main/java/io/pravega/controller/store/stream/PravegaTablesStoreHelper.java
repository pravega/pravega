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
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.IteratorStateImpl;
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
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PravegaTablesStoreHelper {
    private final SegmentHelper segmentHelper;
    private final Executor executor;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper, Executor executor) {
        this.segmentHelper = segmentHelper;
        this.executor = executor;
    }

    public CompletableFuture<Void> createTable(String scope, String tableName) {
        return Futures.toVoid(runOnExecutorWithExceptionHandling(() -> segmentHelper.createTableSegment(scope, tableName, RequestTag.NON_EXISTENT_ID),
                "create table: " + scope + "/" + tableName));
    }

    public CompletableFuture<Boolean> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        return Futures.exceptionallyExpecting(runOnExecutorWithExceptionHandling(
                () -> segmentHelper.deleteTableSegment(scope, tableName, mustBeEmpty, RequestTag.NON_EXISTENT_ID),
                "delete table: " + scope + "/" + tableName),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException, false);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, @NonNull byte[] value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), KeyVersion.NOT_EXISTS), value);
        entries.add(entry);
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID),
                "addNewEntry: key:" + key + " table: " + scope + "/" + tableName)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Version> addNewEntryIfAbsent(String scope, String tableName, String key, @NonNull byte[] value) {
        // if entry exists, we will get write conflict in attempting to create it again. 
        return Futures.exceptionallyExpecting(addNewEntry(scope, tableName, key, value),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, null);
    }

    public CompletableFuture<Version> updateEntry(String scope, String tableName, String key, Data value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        KeyVersionImpl version = value.getVersion() == null ? null :
                new KeyVersionImpl(value.getVersion().asLongVersion().getLongValue());
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), version), value.getData());
        entries.add(entry);
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID),
                "updateEntry: key:" + key + " table: " + scope + "/" + tableName)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTable(scope, tableName, keys, RequestTag.NON_EXISTENT_ID),
                "get entry: key:" + key + " table: " + scope + "/" + tableName)
                .thenApply(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                });
    }

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.removeTableKeys(scope, tableName, keys, 0L),
                "remove entry: key:" + key + " table: " + scope + "/" + tableName);
    }

    public CompletableFuture<Void> removeEntries(String scope, String tableName, List<String> key) {
        List<TableKey<byte[]>> keys = key.stream().map(x -> new TableKeyImpl<>(x.getBytes(), null)).collect(Collectors.toList());
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.removeTableKeys(scope, tableName, keys, 0L),
                "remove entries: keys:" + keys + " table: " + scope + "/" + tableName);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String scope, String tableName, ByteBuf continuationToken) {
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTableKeys(scope, tableName, 1000, 
                IteratorState.fromBytes(continuationToken), 0L)
                .thenApply(result -> {
                    List<String> items = result.getItems().stream().map(x -> new String(x.getKey())).collect(Collectors.toList());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }), "get keys paginated for table:" + scope + "/" + tableName);
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<Pair<String, Data>>>> getEntriesPaginated(String scope, String tableName, 
                                                                                               ByteBuf continuationToken) {
        return runOnExecutorWithExceptionHandling(() -> segmentHelper.readTableEntries(scope, tableName, 1000, 
                IteratorState.fromBytes(continuationToken), 0L)
                .thenApply(result -> {
                    List<Pair<String, Data>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey());
                        Data value = new Data(x.getValue(), new Version.LongVersion(x.getKey().getVersion().getSegmentVersion()));
                        return new ImmutablePair<>(key, value);
                    }).collect(Collectors.toList());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }), "get entries paginated for table:" + scope + "/" + tableName);
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(scope, tableName, token)
                .thenApply(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue())),
                IteratorState.EMPTY.toBytes());
    }

    public AsyncIterator<Pair<String, Data>> getAllEntries(String scope, String tableName) {
        return new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(scope, tableName, token)
                .thenApply(result -> new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue())),
                IteratorState.EMPTY.toBytes());
    }

    private <T> CompletableFuture<T> translateException(CompletableFuture<T> future, String errorMessage) {
        return future.exceptionally(e -> {
            Throwable cause = Exceptions.unwrap(e);
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
            } else {
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    private <T> CompletableFuture<T> runOnExecutorWithExceptionHandling(Supplier<CompletableFuture<T>> futureSupplier, String errorMessage) {
        return CompletableFuture.completedFuture(null)
                                .thenComposeAsync(v -> translateException(futureSupplier.get(), errorMessage), executor);
    }
}
