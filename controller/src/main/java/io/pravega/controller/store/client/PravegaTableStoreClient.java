/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
 * Pravega Table Store client.
 */
public class PravegaTableStoreClient implements StoreClient {
    private final CuratorFramework client;

    PravegaTableStoreClient(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public CuratorFramework getClient() {
        return this.client;
    }

    @Override
    public StoreType getType() {
        return StoreType.PravegaTable;
    }

    @Override
    public void close() {
        client.close();
    }
}
