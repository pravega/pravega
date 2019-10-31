/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.checkpoint;

import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for creating checkpoint store.
 */
public class CheckpointStoreFactory {

    public static CheckpointStore create(StoreClient storeClient) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryCheckpointStore();
            case PravegaTable: 
            case Zookeeper:
                return new ZKCheckpointStore((CuratorFramework) storeClient.getClient());
            default:
                throw new IllegalArgumentException();
        }
    }

    @VisibleForTesting
    public static CheckpointStore createZKStore(final CuratorFramework client) {
        return new ZKCheckpointStore(client);
    }

    @VisibleForTesting
    public static CheckpointStore createInMemoryStore() {
        return new InMemoryCheckpointStore();
    }

}
