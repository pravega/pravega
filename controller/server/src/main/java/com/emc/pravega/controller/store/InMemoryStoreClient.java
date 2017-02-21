/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store;

/**
 * In memory store client.
 */
public class InMemoryStoreClient implements StoreClient {

    @Override
    public Object getClient() {
        return null;
    }

    @Override
    public StoreClientFactory.StoreType getType() {
        return StoreClientFactory.StoreType.InMemory;
    }
}
