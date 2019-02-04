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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.util.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAMS_IN_SCOPE_ROOT_PATH = "/store/streamsinscope/%s";
    private static final int SET_COUNT = 100;
    
    private final String scopePath;
    private final String streamsInScopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    protected ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        scopePath = String.format(SCOPE_PATH, scopeName);
        streamsInScopePath = String.format(STREAMS_IN_SCOPE_ROOT_PATH, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        return store.addNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return store.deleteNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> addStreamToScope(String name, long creationTime) {
        Integer set = findSetForStream(name);
        String setPath = getSetPath(set);
        String path = ZKPaths.makePath(setPath, Scope.encodeStreamInScope(name, creationTime));
        return Futures.toVoid(store.createZNodeIfNotExist(path));
    }
    
    @Override
    public CompletableFuture<Void> removeStreamFromScope(String name, long creationTime) {
        Integer set = findSetForStream(name);
        String setPath = getSetPath(set);
        String path = ZKPaths.makePath(setPath, Scope.encodeStreamInScope(name, creationTime));
        return Futures.toVoid(store.deletePath(path, true));
    }
    
    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        return store.getChildren(scopePath);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        AtomicInteger set = new AtomicInteger(0);
        AtomicReference<String> floor = new AtomicReference<>();
        if (!Strings.isNullOrEmpty(continuationToken)) {
            Pair<String, Long> decoded = Scope.decodeStreamInScope(continuationToken);
            set.set(findSetForStream(decoded.getKey()));
            floor.set(continuationToken);
        } else {
            floor.set(Scope.encodeStreamInScope("", 0L));
        }
        
        List<String> taken = new LinkedList<>();
        // start with initial set. fetch all children over floor

        Supplier<CompletableFuture<Void>> supplier = () -> getStreamsInSet(set.get(), Config.LIST_STREAM_LIMIT - taken.size(), floor.get())
                .thenAccept(encodedStreams -> {
                    if (!encodedStreams.isEmpty()) {
                        // we found streams in this set to be included. 
                        taken.addAll(encodedStreams);
                    }
                    // reset the floor. 
                    floor.set(Scope.encodeStreamInScope("", 0L));
                    // try the next set. 
                    set.incrementAndGet();
                });
        
        
        return Futures.loop(() -> taken.size() < Config.LIST_STREAM_LIMIT && set.get() < 100, supplier, executor)
                      .thenApply(v -> {
                          List<String> list = taken.stream().map(x -> Scope.decodeStreamInScope(x).getKey()).collect(Collectors.toList());
                          // set next continuation token as the last element in this list
                          String nextContinuationToken = taken.isEmpty() || taken.size() < limit ? "" : taken.get(list.size() - 1);
                          return new ImmutablePair<>(list, nextContinuationToken);
               });
    }

    private String getSetPath(Integer set) {
        return ZKPaths.makePath(streamsInScopePath, set.toString());
    }

    private int findSetForStream(String stream) {
        return Math.abs(stream.hashCode()) % SET_COUNT;
    }

    private CompletableFuture<List<String>> getStreamsInSet(Integer set, int limit, String continuationToken) {
        String setPath = getSetPath(set);
        return store.getChildren(setPath)
                    .exceptionally(e -> {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            return Collections.emptyList();
                        } else {
                            throw new CompletionException(e);
                        }
                    })
                    .thenApply(children -> children.stream()
                                               .filter(x -> Scope.compareStreamInScope(x, continuationToken) > 0)
                                               .limit(limit).collect(Collectors.toList()));
    }
    
    @Override
    public void refresh() {
    }
}
