/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * InMemory implementation of Scope.
 */
public class InMemoryScope implements Scope {

    private final String scopeName;

    private List<String> streamsInScope;

    InMemoryScope(String scopeName) {
        this.scopeName = scopeName;
    }

    /**
     * A getter method for streamsInScope.
     *
     * @return a copy of streamsInScope list to avoid synchronization issues.
     */
    public synchronized List<String> getStreamsInScope() {
        return new ArrayList<>(this.streamsInScope);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public synchronized CompletableFuture<Void> createScope() {
        this.streamsInScope = new ArrayList<>();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> deleteScope() {
        this.streamsInScope.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<List<String>> listStreamsInScope() {
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
    public synchronized void addStreamToScope(String stream) {
        this.streamsInScope.add(stream);
    }
}
