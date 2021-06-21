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

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.segmentstore.server.store.StreamSegmentServiceTests;

/**
 * Unit tests for the SynchronousStreamSegmentStore class.
 */
public class SynchronousStreamSegmentStoreTests extends StreamSegmentServiceTests {

    @Override
    protected int getThreadPoolSize() {
        // We await all async operations, which means we'll be eating up a lot of threads for this test.
        return super.getThreadPoolSize() * 50;
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
