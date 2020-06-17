/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.stream.InMemoryStreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;

import java.util.Map;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryKVTMetadataStoreTest extends KVTableMetadataStoreTest {

    @Override
    public void setupStore() throws Exception {
        store = KVTableStoreFactory.createInMemoryStore(executor);
        this.streamStore = StreamStoreFactory.createInMemoryStore(executor);
        Map<String, InMemoryScope> scopeMap = ((InMemoryStreamMetadataStore) streamStore).getScopes();
        ((InMemoryKVTMetadataStore) store).setScopes(scopeMap);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
    }

    @Override
<<<<<<< HEAD
    Controller.CreateScopeStatus createScope(String scopeName) throws Exception {
        return streamStore.createScope(scopeName).get();
=======
    Controller.CreateScopeStatus createScope() throws Exception {
        return streamStore.createScope(scope).get();
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
    }
}
