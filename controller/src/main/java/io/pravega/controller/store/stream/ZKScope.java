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

import org.apache.curator.utils.ZKPaths;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pravega.controller.store.stream.ZKStreamMetadataStore.SCOPE_ROOT_PATH;

public class ZKScope implements Scope {
    private final String scopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        scopePath = ZKPaths.makePath(SCOPE_ROOT_PATH, scopeName);
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
        return store.getChildren(scopePath);
    }

    @Override
    public void refresh() {
    }

}
