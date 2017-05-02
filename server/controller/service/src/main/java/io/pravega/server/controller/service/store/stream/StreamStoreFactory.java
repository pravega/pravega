/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream;

import io.pravega.server.controller.service.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
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
            default:
                throw new NotImplementedException();
        }
    }

    @VisibleForTesting
    public static StreamMetadataStore createZKStore(final CuratorFramework client,
                                                    final ScheduledExecutorService executor) {
        return new ZKStreamMetadataStore(client, executor);
    }

    @VisibleForTesting
    public static StreamMetadataStore createInMemoryStore(final ScheduledExecutorService executor) {
        return new InMemoryStreamMetadataStore(executor);
    }
}
