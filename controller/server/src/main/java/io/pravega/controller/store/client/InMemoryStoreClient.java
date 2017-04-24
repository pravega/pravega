/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
