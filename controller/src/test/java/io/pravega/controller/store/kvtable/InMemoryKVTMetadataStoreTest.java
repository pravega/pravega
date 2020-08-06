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

import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryKVTMetadataStoreTest extends KVTableMetadataStoreTest {

    @Override
    public void setupStore() throws Exception {
        this.streamStore = StreamStoreFactory.createInMemoryStore(executor);
        store = KVTableStoreFactory.createInMemoryStore(this.streamStore, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
    }

    @Override
    Controller.CreateScopeStatus createScope(String scopeName) throws Exception {
        return streamStore.createScope(scopeName).get();
    }
}
