/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class KVTableStoreFactory {

    public static KVTableMetadataStore createStore(final StoreClient storeClient, final SegmentHelper segmentHelper,
                                                   final GrpcAuthHelper authHelper, final ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case PravegaTable:
                return new PravegaTablesKVTMetadataStore(segmentHelper, (CuratorFramework) storeClient.getClient(), executor, authHelper);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    @VisibleForTesting
    public static KVTableMetadataStore createPravegaTablesStore(final SegmentHelper segmentHelper, final GrpcAuthHelper authHelper,
                                                                final CuratorFramework client, final ScheduledExecutorService executor) {
        return new PravegaTablesKVTMetadataStore(segmentHelper, client, executor, authHelper);
    }
    
    @VisibleForTesting
    public static KVTableMetadataStore createZKStore(final CuratorFramework client, final ScheduledExecutorService executor) {
        throw new UnsupportedOperationException("ZKStore not supported for KeyValueTables");
    }
    
    @VisibleForTesting
    public static KVTableMetadataStore createInMemoryStore(final Executor executor) {
        throw new UnsupportedOperationException("InMemoryStore not supported for KeyValueTables");
    }
}
