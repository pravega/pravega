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
import lombok.Synchronized;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * InMemory implementation of Scope.
 */
public class InMemoryScope implements Scope {

    private final String scopeName;

    @GuardedBy("$lock")
    private SortedSet<String> sortedStreamsInScope;

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
        this.sortedStreamsInScope = new TreeSet<>(Scope::compareStreamInScope);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteScope() {
        this.sortedStreamsInScope.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> addStreamToScope(String stream, long creationTime) {
        sortedStreamsInScope.add(Scope.encodeStreamInScope(stream, creationTime));
        
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> removeStreamFromScope(String stream, long creationTime) {
        this.sortedStreamsInScope.remove(Scope.encodeStreamInScope(stream, creationTime));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listStreamsInScope() {
        return CompletableFuture.completedFuture(this.sortedStreamsInScope.stream().map(x -> Scope.decodeStreamInScope(x).getKey()).collect(Collectors.toList()));
    }
    
    @Override
    @Synchronized
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        String newContinuationToken;
        List<String> limited;
        synchronized (this) {
            if (Strings.isNullOrEmpty(continuationToken)) {
                limited = sortedStreamsInScope.stream().limit(limit).collect(Collectors.toList());
            } else {
                // find in list based on time
                limited = sortedStreamsInScope.tailSet(continuationToken)
                                              .stream().limit(limit).collect(Collectors.toList());
                if (!limited.isEmpty()) {
                    limited.remove(0);
                }
            }
            
            if (limited.isEmpty() || limited.size() < limit) {
                newContinuationToken = "";
            } else {
                newContinuationToken = limited.get(limited.size() - 1);      
            }
        }

        List<String> result = limited.stream().map(x -> Scope.decodeStreamInScope(x).getKey()).collect(Collectors.toList());
        
        return CompletableFuture.completedFuture(new ImmutablePair<>(result, newContinuationToken));
    }

    @Override
    public void refresh() {

    }
}
