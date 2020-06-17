/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.client;

/**
 * In memory store client.
 */
class InMemoryStoreClient implements StoreClient {

    @Override
    public Object getClient() {
        return null;
    }

    @Override
    public StoreType getType() {
        return StoreType.InMemory;
    }

    @Override
    public void close() throws Exception {

    }
}
