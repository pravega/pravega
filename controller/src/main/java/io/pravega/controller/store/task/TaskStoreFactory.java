/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.task;

import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Task store factory.
 */
public class TaskStoreFactory {

    public static TaskMetadataStore createStore(StoreClient storeClient, ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case Zookeeper:
            case PravegaTable:
                return new ZKTaskMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case InMemory:
                return new InMemoryTaskMetadataStore(executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
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
