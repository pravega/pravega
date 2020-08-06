/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.kvtable.InMemoryKVTable;
import io.pravega.controller.store.kvtable.KeyValueTable;
import lombok.Synchronized;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import io.pravega.controller.store.stream.StoreException;

/**
 * InMemory implementation of Scope.
 */
public class InMemoryScope implements Scope {

    private final String scopeName;

    @GuardedBy("$lock")
    private TreeMap<Integer, String> sortedStreamsInScope;
    private HashMap<String, Integer> streamsPositionMap;

    @GuardedBy("$lock")
    private final TreeMap<String, InMemoryKVTable> kvTablesMap =  new TreeMap<String, InMemoryKVTable>();

    public InMemoryScope(String scopeName) {
        this.scopeName = scopeName;
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> createScope() {
        this.sortedStreamsInScope = new TreeMap<>(Integer::compare);
        this.streamsPositionMap = new HashMap<>();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteScope() {
        this.sortedStreamsInScope.clear();
        this.sortedStreamsInScope = null;
        this.streamsPositionMap.clear();
        this.streamsPositionMap = null;

        this.kvTablesMap.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    public CompletableFuture<Void> addStreamToScope(String stream) {
        int next = streamsPositionMap.size();
        streamsPositionMap.putIfAbsent(stream, next);
        Integer position = streamsPositionMap.get(stream);
        sortedStreamsInScope.put(position, stream);

        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    public CompletableFuture<Void> removeStreamFromScope(String stream) {
        Integer position = streamsPositionMap.get(stream);
        if (position != null) {
            this.sortedStreamsInScope.remove(position);
            this.streamsPositionMap.remove(stream);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listStreamsInScope() {
        return CompletableFuture.completedFuture(Lists.newArrayList(this.sortedStreamsInScope.values()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor) {
        String newContinuationToken;
        List<Map.Entry<Integer, String>> limited;
        synchronized (this) {
            if (sortedStreamsInScope == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope not found"));
            }
            if (Strings.isNullOrEmpty(continuationToken)) {
                limited = sortedStreamsInScope.entrySet().stream().limit(limit).collect(Collectors.toList());
            } else {
                int lastPos = Strings.isNullOrEmpty(continuationToken) ? 0 : Integer.parseInt(continuationToken);
                limited = sortedStreamsInScope.tailMap(lastPos, false).entrySet()
                                              .stream().limit(limit).collect(Collectors.toList());
            }

            if (limited.isEmpty()) {
                newContinuationToken = continuationToken;
            } else {
                newContinuationToken = limited.get(limited.size() - 1).getKey().toString();
            }
        }

        List<String> result = limited.stream().map(Map.Entry::getValue).collect(Collectors.toList());

        return CompletableFuture.completedFuture(new ImmutablePair<>(result, newContinuationToken));
    }

    @Synchronized
    public CompletableFuture<Void> addKVTableToScope(String kvt, byte[] id) {
        kvTablesMap.putIfAbsent(kvt, new InMemoryKVTable(this.scopeName, kvt, BitConverter.readUUID(id, 0)));
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    public boolean checkTableExists(String kvt) {
        return kvTablesMap.containsKey(kvt);
    }

    @Override
    public void refresh() {
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(int limit, String continuationToken, Executor executor) {
        if (kvTablesMap.size() == 0) {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, this.scopeName));
        }
        List<String> sortedKVTablesList = kvTablesMap.keySet().stream().collect(Collectors.toList());
        int start = 0;
        if (!continuationToken.isEmpty()) {
            start = Integer.parseInt(continuationToken);
        }

        int end = ((start + limit) >= sortedKVTablesList.size()) ? sortedKVTablesList.size() : start + limit;
        List<String> nextBatchOfTables = sortedKVTablesList.subList(start, end);

        return CompletableFuture.completedFuture(new ImmutablePair<>(nextBatchOfTables, String.valueOf(end)));
    }

    @Synchronized
    public CompletableFuture<Void> removeKVTableFromScope(String kvtName) {
        kvTablesMap.remove(kvtName);
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    public KeyValueTable getKeyValueTable(String name) {
        return kvTablesMap.get(name);
    }
}
