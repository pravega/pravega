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
package io.pravega.controller.store.checkpoint;

import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

/**
 * Factory for creating checkpoint store.
 */
public class CheckpointStoreFactory {

    public static CheckpointStore create(StoreClient storeClient) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryCheckpointStore();
            case PravegaTable: 
            case Zookeeper:
                return new ZKCheckpointStore((CuratorFramework) storeClient.getClient());
            default:
                throw new IllegalArgumentException();
        }
    }

    @VisibleForTesting
    public static CheckpointStore createZKStore(final CuratorFramework client) {
        return new ZKCheckpointStore(client);
    }

    @VisibleForTesting
    public static CheckpointStore createInMemoryStore() {
        return new InMemoryCheckpointStore();
    }

}
