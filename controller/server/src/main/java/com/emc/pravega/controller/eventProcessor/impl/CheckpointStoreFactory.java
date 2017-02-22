/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for creating checkpoint store.
 */
public class CheckpointStoreFactory {

    static CheckpointStore create(CheckpointConfig checkpointConfig) {
        final CheckpointConfig.StoreType type = checkpointConfig.getStoreType();
        final Object client = checkpointConfig.getCheckpointStoreClient();
        switch (type) {
            case InMemory:
                return new InMemoryCheckpointStore();
            case Zookeeper:
                return new ZKCheckpointStore((CuratorFramework) client);
            default:
                return null;
        }
    }

}
