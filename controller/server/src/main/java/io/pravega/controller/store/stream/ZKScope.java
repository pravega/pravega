/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private final String scopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    protected ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        scopePath = String.format(SCOPE_PATH, scopeName);
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
    public CompletableFuture<List<String>> listStreamsInScope() {
        return store.getStreamsInPath(scopePath);
    }

    @Override
    public void refresh() {
    }

}
