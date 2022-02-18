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

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

@RequiredArgsConstructor

public class SlowStorageFactory implements SimpleStorageFactory {
    @Getter
    final protected ScheduledExecutorService executor;

    @Getter
    final protected StorageFactory inner;

    @Getter
    final protected Duration duration;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        if (inner instanceof SimpleStorageFactory) {
            val innerStorage = ((SimpleStorageFactory) inner).createStorageAdapter(containerId, metadataStore);
            return new SlowStorage(innerStorage, executor, duration);
        } else {
            throw new UnsupportedOperationException("inner is not SimpleStorageFactory");
        }
    }

    @Override
    public Storage createStorageAdapter() {
        return new SlowStorage(inner.createStorageAdapter(), executor, duration);
    }

    @Override
    public ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig() {
        if (inner instanceof SimpleStorageFactory) {
            return ((SimpleStorageFactory) inner).getChunkedSegmentStorageConfig();
        } else {
            throw new UnsupportedOperationException("inner is not SimpleStorageFactory");
        }
    }

    @Override
    public SyncStorage createSyncStorage() {
        if (inner instanceof SimpleStorageFactory) {
            throw new UnsupportedOperationException("SimpleStorageFactory does not support createSyncStorage");
        } else {
            return inner.createSyncStorage();
        }
    }
}
