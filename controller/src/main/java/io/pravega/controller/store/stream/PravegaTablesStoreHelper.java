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
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class PravegaTablesStoreHelper {
    private final SegmentHelper segmentHelper;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper) {
        this.segmentHelper = segmentHelper;
    }
    
    public CompletableFuture<Void> createTable(String scope, String tableName) {
        return Futures.toVoid(segmentHelper.createTableSegment(scope, tableName, RequestTag.NON_EXISTENT_ID));
    }

    public CompletableFuture<Boolean> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        return Futures.exceptionallyExpecting(handleException(segmentHelper.deleteTableSegment(scope, tableName, mustBeEmpty, RequestTag.NON_EXISTENT_ID)),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, false);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, @NonNull byte[] value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), KeyVersion.NOT_EXISTS), value);
        entries.add(entry);
        return handleException(segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID))
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
        return handleException(segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID))
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return handleException(segmentHelper.readTable(scope, tableName, keys, RequestTag.NON_EXISTENT_ID))
                .thenApply(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                });
    }

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return handleException(segmentHelper.removeTableKeys(scope, tableName, keys, 0L));
    }
    
    public CompletableFuture<Void> removeEntries(String scope, String tableName, List<String> key) {
        List<TableKey<byte[]>> keys = key.stream().map(x -> new TableKeyImpl<>(x.getBytes(), null)).collect(Collectors.toList());
        return handleException(segmentHelper.removeTableKeys(scope, tableName, keys, 0L));
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }

    public AsyncIterator<Pair<String, Data>> getAllEntries(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }

    private <T> CompletableFuture<T> handleException(CompletableFuture<T> future) {
        return future.exceptionally(e -> {
            Throwable cause = Exceptions.unwrap(e);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                    case UnknownHost:
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, "");
                        break;
                    case PreconditionFailed:
                        toThrow = StoreException.create(StoreException.Type.ILLEGAL_STATE, wcfe, "");
                        break;
                    case AuthFailed:
                        toThrow = StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, "");
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, "");
                        break;
                    case TableSegmentNotEmpty:
                        toThrow = StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, wcfe, "");
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, "");
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreException.create(StoreException.Type.WRITE_CONFLICT, wcfe, "");
                        break;
                    default:
                        toThrow = StoreException.create(StoreException.Type.UNKNOWN, wcfe, "");
                }
            } else {
                toThrow = StoreException.create(StoreException.Type.UNKNOWN, cause, "");
            }

            throw new CompletionException(toThrow);
        });
    }
}
