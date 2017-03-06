/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

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
}
