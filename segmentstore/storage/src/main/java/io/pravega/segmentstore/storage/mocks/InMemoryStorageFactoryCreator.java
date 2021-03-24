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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

public class InMemoryStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor) {
        if (storageFactoryInfo.getStorageLayoutType().equals(StorageLayoutType.CHUNKED_STORAGE)) {
            val factory = new InMemorySimpleStorageFactory(setup.getConfig(ChunkedSegmentStorageConfig::builder), executor, true);
            return factory;
        } else {
            InMemoryStorageFactory factory = new InMemoryStorageFactory(executor);
            return factory;
        }

    }

    @Override
    public StorageFactoryInfo[] getStorageFactories() {
        return new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("INMEMORY")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("INMEMORY")
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build(),
        };
    }

}
