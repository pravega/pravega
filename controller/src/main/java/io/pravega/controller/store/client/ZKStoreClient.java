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
    public StoreType getType() {
        return StoreType.Zookeeper;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
