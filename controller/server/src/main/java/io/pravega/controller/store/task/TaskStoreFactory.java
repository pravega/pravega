/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.task;

import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Task store factory.
 */
public class TaskStoreFactory {

    public static TaskMetadataStore createStore(StoreClient storeClient, ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case Zookeeper:
                return new ZKTaskMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case InMemory:
                return new InMemoryTaskMetadataStore(executor);
            default:
                throw new NotImplementedException();
        }
    }

    @VisibleForTesting
    public static TaskMetadataStore createZKStore(final CuratorFramework client,
                                                  final ScheduledExecutorService executor) {
        return new ZKTaskMetadataStore(client, executor);
    }

    @VisibleForTesting
    public static TaskMetadataStore createInMemoryStore(final ScheduledExecutorService executor) {
        return new InMemoryTaskMetadataStore(executor);
    }
}
