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
package io.pravega.segmentstore.server.store;

import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for the StreamSegmentService class.
 */
public class StreamSegmentServiceTests extends StreamSegmentStoreTestBase {
    private InMemoryStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    @Before
    public void setUp() {
        this.storageFactory = new InMemoryStorageFactory(executorService());
        this.durableDataLogFactory = new PermanentDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.storageFactory != null) {
            this.storageFactory.close();
            this.storageFactory = null;
        }
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId, boolean useChunkedSegmentStorage) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                             .withStorageFactory(setup -> this.storageFactory)
                             .withDataLogFactory(setup -> this.durableDataLogFactory);
    }

    public static class PermanentDurableDataLogFactory extends InMemoryDurableDataLogFactory {
        PermanentDurableDataLogFactory(ScheduledExecutorService executorService) {
            super(executorService);
        }

        @Override
        public void close() {
            // This method intentionally left blank; we want this factory to live between multiple recovery attempts.
        }
    }
}
