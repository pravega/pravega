/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.checkpoint;

import com.emc.pravega.controller.store.client.StoreClient;
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
            case Zookeeper:
                return new ZKCheckpointStore((CuratorFramework) storeClient.getClient());
            default:
                return null;
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
