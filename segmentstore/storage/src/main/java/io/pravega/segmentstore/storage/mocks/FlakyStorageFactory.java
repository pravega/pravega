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
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

@RequiredArgsConstructor

public class FlakyStorageFactory implements SimpleStorageFactory {
    @Getter
    final protected ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @Getter
    final protected ScheduledExecutorService executor;

    @Getter
    final protected SimpleStorageFactory inner;

    @Getter
    final protected Duration duration;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        return new FlakyStorage(inner.createStorageAdapter(containerId, metadataStore), executor, duration);
    }

    @Override
    public Storage createStorageAdapter() {
        return new FlakyStorage(inner.createStorageAdapter(), executor, duration);
    }
}
