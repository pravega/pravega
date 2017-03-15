/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.client.StoreClient;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreFactory {
    public static StreamMetadataStore createStore(final StoreClient storeClient, final ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryStreamMetadataStore(executor);
            case Zookeeper:
                return new ZKStreamMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
        }
    }

    public static StreamMetadataStore createZKStore(final CuratorFramework client,
                                                    final ScheduledExecutorService executor) {
        return new ZKStreamMetadataStore(client, executor);
    }

    public static StreamMetadataStore createInMemoryStore(final ScheduledExecutorService executor) {
        return new InMemoryStreamMetadataStore(executor);
    }
}
