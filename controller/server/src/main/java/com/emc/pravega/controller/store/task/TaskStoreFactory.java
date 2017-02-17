/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Task store factory.
 */
public class TaskStoreFactory {

    public static TaskMetadataStore createStore(StoreClient storeClient, ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case Zookeeper:
                return new ZKTaskMetadataStore((ZKStoreClient) storeClient, executor);
            case InMemory:
                return new InMemoryTaskMetadataStore(executor);
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
        }
    }
}
