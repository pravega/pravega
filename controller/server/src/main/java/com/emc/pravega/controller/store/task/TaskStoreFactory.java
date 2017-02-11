/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
