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
package io.pravega.segmentstore.server.mocks;

import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.segmentstore.server.store.StreamSegmentServiceTests;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the SynchronousStreamSegmentStore class.
 */
public class SynchronousStreamSegmentStoreTests extends StreamSegmentServiceTests {

    @Override
    protected int getThreadPoolSize() {
        // We await all async operations, which means we'll be eating up a lot of threads for this test.
        return super.getThreadPoolSize() * 50;
    }

    @Test
    public void testGetExtendedChunkInfo() {
        ExtendedChunkInfo chunk = ExtendedChunkInfo.builder()
                .lengthInMetadata(10)
                .lengthInStorage(10)
                .startOffset(10)
                .chunkName("chunk")
                .existsInStorage(false)
                .build();
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        when(store.getExtendedChunkInfo(anyString(), any())).thenReturn(CompletableFuture.completedFuture(List.of(chunk)));

        SynchronousStreamSegmentStore syncStore = new SynchronousStreamSegmentStore(store);
        assertEquals(chunk, syncStore.getExtendedChunkInfo("segment", Duration.ofSeconds(1)).join().get(0));
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId, boolean useChunkedSegmentStorage) {
        return super.createBuilder(builderConfig, instanceId, useChunkedSegmentStorage)
                    .withStreamSegmentStore(setup -> {
                        StreamSegmentStore base = new StreamSegmentService(setup.getContainerRegistry(), setup.getSegmentToContainerMapper());
                        return new SynchronousStreamSegmentStore(base);
                    });
    }
}
