/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.client;

/**
 * In memory store client.
 */
class InMemoryStoreClient implements StoreClient {

    InMemoryStoreClient() {
    }

    @Override
    public Object getClient() {
        return null;
    }

    @Override
    public StoreType getType() {
        return StoreType.InMemory;
    }
}
