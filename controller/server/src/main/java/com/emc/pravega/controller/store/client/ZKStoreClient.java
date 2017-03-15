/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.client;

import org.apache.curator.framework.CuratorFramework;

/**
 * ZK client.
 */
class ZKStoreClient implements StoreClient {

    private final CuratorFramework client;

    ZKStoreClient(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public CuratorFramework getClient() {
        return this.client;
    }

    @Override
    public StoreClientFactory.StoreType getType() {
        return StoreClientFactory.StoreType.Zookeeper;
    }
}
