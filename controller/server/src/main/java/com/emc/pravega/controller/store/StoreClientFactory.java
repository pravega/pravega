/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store;

import org.apache.commons.lang.NotImplementedException;

/**
 * Factory method for store clients.
 */
public class StoreClientFactory {

    public enum StoreType {
        InMemory,
        Zookeeper,
        ECS,
        S3,
        HDFS
    }

    public static StoreClient createStoreClient(final StoreType type) {
        switch (type) {
            case Zookeeper:
                return new ZKStoreClient();
            case InMemory:
                return new InMemoryStoreClient();
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
        }
    }
}
