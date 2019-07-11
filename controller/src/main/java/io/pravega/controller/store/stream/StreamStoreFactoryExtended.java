/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.client.StoreClient;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreFactoryExtended extends StreamStoreFactory {
    public static ExtendedStreamMetadataStore createStore(final StoreClient storeClient, final SegmentHelper segmentHelper,
                                                          final AuthHelper authHelper, final ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryStreamMetadataStore(executor);
            case Zookeeper:
                return new ZKStreamMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case PravegaTable:
                return new PravegaTablesStreamMetadataStore(segmentHelper, (CuratorFramework) storeClient.getClient(), executor, authHelper);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    @VisibleForTesting
    public static ExtendedStreamMetadataStore createPravegaTablesStore(final SegmentHelper segmentHelper, final AuthHelper authHelper,
                                                                       final CuratorFramework client, final ScheduledExecutorService executor) {
        return new PravegaTablesStreamMetadataStore(segmentHelper, client, executor, authHelper);
    }

    @VisibleForTesting
    public static ExtendedStreamMetadataStore createZKStore(final CuratorFramework client, final ScheduledExecutorService executor) {
        return new ZKStreamMetadataStore(client, executor);
    }

    @VisibleForTesting
    public static ExtendedStreamMetadataStore createInMemoryStore(final Executor executor) {
        return new InMemoryStreamMetadataStore(executor);
    }
}
