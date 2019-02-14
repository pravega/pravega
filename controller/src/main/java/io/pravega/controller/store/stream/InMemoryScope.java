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

import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * InMemory implementation of Scope.
 */
public class InMemoryScope implements Scope {

    private final String scopeName;

    @GuardedBy("$lock")
    private List<String> streamsInScope;

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
        this.streamsInScope = new ArrayList<>();
        return CompletableFuture.completedFuture(null);
    }
    
    @Override
    @Synchronized
    public CompletableFuture<Void> deleteScope() {
        this.streamsInScope.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listStreamsInScope() {
        return CompletableFuture.completedFuture(new ArrayList<>(this.streamsInScope));
    }

    @Override
    public void refresh() {

    }

    /**
     * Adds stream name to the scope.
     *
     * @param stream Name of stream to be added.
     */
    @Synchronized
    public void addStreamToScope(String stream) {
        this.streamsInScope.add(stream);
    }

    @Synchronized
    public void removeStreamFromScope(String stream) {
        this.streamsInScope.remove(stream);
    }
}
