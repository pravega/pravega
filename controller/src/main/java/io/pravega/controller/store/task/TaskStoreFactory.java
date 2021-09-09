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
