/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.util.Config;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreFactory {
    public static final ImmutableMap<BucketStore.ServiceType, Integer> BUCKET_COUNT_MAP = ImmutableMap.of(
            BucketStore.ServiceType.RetentionService, Config.RETENTION_BUCKET_COUNT,
            BucketStore.ServiceType.WatermarkingService, Config.WATERMARKING_BUCKET_COUNT);

    public static StreamMetadataStore createStore(final StoreClient storeClient, final SegmentHelper segmentHelper,
                                                  final GrpcAuthHelper authHelper, final ScheduledExecutorService executor) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryStreamMetadataStore();
            case Zookeeper:
                return new ZKStreamMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case PravegaTable:
                return new PravegaTablesStreamMetadataStore(segmentHelper, (CuratorFramework) storeClient.getClient(),
                        executor, authHelper);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    @VisibleForTesting
    public static StreamMetadataStore createPravegaTablesStore(final SegmentHelper segmentHelper, final GrpcAuthHelper authHelper,
                                                               final CuratorFramework client, final ScheduledExecutorService executor) {
        return new PravegaTablesStreamMetadataStore(segmentHelper, client, executor, authHelper);
    }
    
    @VisibleForTesting
    public static StreamMetadataStore createZKStore(final CuratorFramework client, final ScheduledExecutorService executor) {
        return new ZKStreamMetadataStore(client, executor);
    }
    
    @VisibleForTesting
    public static StreamMetadataStore createInMemoryStore() {
        return new InMemoryStreamMetadataStore();
    }

    public static BucketStore createBucketStore(final StoreClient storeClient, final Executor executor) {
        switch (storeClient.getType()) {
            case InMemory: 
                return createInMemoryBucketStore();
            case Zookeeper: 
            case PravegaTable:
                return createZKBucketStore((CuratorFramework) storeClient.getClient(), executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }
    
    @VisibleForTesting
    public static BucketStore createZKBucketStore(final CuratorFramework client, final Executor executor) {
        return createZKBucketStore(BUCKET_COUNT_MAP, client, executor);
    }

    @VisibleForTesting
    public static BucketStore createZKBucketStore(final ImmutableMap<BucketStore.ServiceType, Integer> map, 
                                                  final CuratorFramework client, final Executor executor) {
        return new ZookeeperBucketStore(map, client, executor);
    }
    
    @VisibleForTesting
    public static BucketStore createInMemoryBucketStore() {
        return createInMemoryBucketStore(BUCKET_COUNT_MAP);
    }

    @VisibleForTesting
    public static BucketStore createInMemoryBucketStore(ImmutableMap<BucketStore.ServiceType, Integer> map) {
        return new InMemoryBucketStore(map);
    }
}
