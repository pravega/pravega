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
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.stream.StreamMetadataStore;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.ScheduledExecutorService;

public class KVTableStoreFactory {

    public static KVTableMetadataStore createStore(final StoreClient storeClient, final SegmentHelper segmentHelper,
                                                   final GrpcAuthHelper authHelper, final ScheduledExecutorService executor,
                                                   final StreamMetadataStore streamStore) {
        switch (storeClient.getType()) {
            case PravegaTable:
                return new PravegaTablesKVTMetadataStore(segmentHelper, (CuratorFramework) storeClient.getClient(), executor, authHelper);
            case InMemory:
                return new InMemoryKVTMetadataStore(streamStore);
            case Zookeeper:
                return new ZookeeperKVTMetadataStore((CuratorFramework) storeClient.getClient(), executor);
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
        return new ZookeeperKVTMetadataStore(client, executor);
    }
    
    @VisibleForTesting
    public static KVTableMetadataStore createInMemoryStore(final StreamMetadataStore streamStore, final ScheduledExecutorService executor) {
        return new InMemoryKVTMetadataStore(streamStore);
    }
}
