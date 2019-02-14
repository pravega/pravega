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
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PravegaTablesStoreHelper {
    
    // TODO: shivesh: error mapping from store errors to controller metadata schema errors 
    private final SegmentHelper segmentHelper;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper) {
        this.segmentHelper = segmentHelper;
    }
    
    public CompletableFuture<Void> createTable(String scope, String tableName) {
        return Futures.toVoid(segmentHelper.createTableSegment(scope, tableName, RequestTag.NON_EXISTENT_ID));
    }

    public CompletableFuture<Boolean> deleteTable(String scope, String tableName, boolean mustBeEmpty) {
        return Futures.exceptionallyExpecting(segmentHelper.deleteTableSegment(scope, tableName, mustBeEmpty, RequestTag.NON_EXISTENT_ID),
                            e -> Exceptions.unwrap(e) instanceof WireCommandFailedException
                                    && ((WireCommandFailedException) e).getReason().equals(
                                            WireCommandFailedException.Reason.TableSegmentNotEmpty), false);
    }

    public CompletableFuture<Version> addNewEntry(String scope, String tableName, String key, byte[] value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), KeyVersion.NOT_EXISTS), value);
        entries.add(entry);
        return segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Version> updateEntry(String scope, String tableName, String key, Data value) {
        List<TableEntry<byte[], byte[]>> entries = new LinkedList<>();
        KeyVersionImpl version = new KeyVersionImpl(value.getVersion().asLongVersion().getLongValue());
        TableEntry<byte[], byte[]> entry = new TableEntryImpl<>(new TableKeyImpl<>(key.getBytes(), version), value.getData());
        entries.add(entry);
        return segmentHelper.updateTableEntries(scope, tableName, entries, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    KeyVersion first = x.get(0);
                    return new Version.LongVersion(first.getSegmentVersion());
                });
    }

    public CompletableFuture<Data> getEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return segmentHelper.readTable(scope, tableName, keys, RequestTag.NON_EXISTENT_ID)
                .thenApply(x -> {
                    TableEntry<byte[], byte[]> first = x.get(0);
                    return new Data(first.getValue(), new Version.LongVersion(first.getKey().getVersion().getSegmentVersion()));
                });
    }

    public CompletableFuture<Void> removeEntry(String scope, String tableName, String key) {
        List<TableKey<byte[]>> keys = new LinkedList<>();
        keys.add(new TableKeyImpl<>(key.getBytes(), null));
        return segmentHelper.removeTableKeys(scope, tableName, keys, 0L);
    }
    
    public CompletableFuture<Void> removeEntries(String scope, String tableName, List<String> key) {
        List<TableKey<byte[]>> keys = key.stream().map(x -> new TableKeyImpl<>(x.getBytes(), null)).collect(Collectors.toList());
        return segmentHelper.removeTableKeys(scope, tableName, keys, 0L);
    }

    public AsyncIterator<String> getAllKeys(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }

    public AsyncIterator<Pair<String, Data>> getAllEntries(String scope, String tableName) {
        throw new UnsupportedOperationException();
    }
}
