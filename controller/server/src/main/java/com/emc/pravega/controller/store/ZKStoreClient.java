/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store;

import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

/**
 * ZK client.
 */
public class ZKStoreClient implements StoreClient {

    private final CuratorFramework client;

    public ZKStoreClient() {
        this.client = ZKUtils.getCuratorClient();
    }

    @VisibleForTesting
    public ZKStoreClient(CuratorFramework client) {
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
