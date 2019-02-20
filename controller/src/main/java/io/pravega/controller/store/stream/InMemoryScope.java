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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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

/**
 * InMemory implementation of Scope.
 */
public class InMemoryScope implements Scope {

    private final String scopeName;

    @GuardedBy("$lock")
    private TreeMap<Integer, String> sortedStreamsInScope;
    private HashMap<String, Integer> streamsPositionMap;

    InMemoryScope(String scopeName) {
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
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        String newContinuationToken;
        List<Map.Entry<Integer, String>> limited;
        synchronized (this) {
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

    @Override
    public void refresh() {

    }
}
