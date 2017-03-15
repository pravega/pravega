/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.client;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

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

    public static StoreClient createStoreClient(final StoreType type, final Object client) {
        switch (type) {
            case Zookeeper:
                Preconditions.checkArgument(client.getClass() == CuratorFramework.class,
                        "client should be of type " + CuratorFramework.class.getCanonicalName());
                return new ZKStoreClient((CuratorFramework) client);
            case InMemory:
                return new InMemoryStoreClient();
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
        }
    }

    public static StoreClient createInMemoryStoreClient() {
        return new InMemoryStoreClient();
    }

    public static StoreClient createZKStoreClient(CuratorFramework client) {
        return new ZKStoreClient(client);
    }
}
