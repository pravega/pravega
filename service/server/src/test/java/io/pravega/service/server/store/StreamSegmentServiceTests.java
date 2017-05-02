/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server.store;

import io.pravega.service.storage.Storage;
import io.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.service.storage.mocks.InMemoryStorageFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;

/**
 * Unit tests for the StreamSegmentService class.
 */
public class StreamSegmentServiceTests extends StreamSegmentStoreTestBase {
    @GuardedBy("this")
    private InMemoryStorageFactory storageFactory;
    @GuardedBy("this")
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    @Override
    protected synchronized ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig, AtomicReference<Storage> storage) {
        if (this.storageFactory == null) {
            this.storageFactory = new InMemoryStorageFactory(executorService());
        }

        if (this.durableDataLogFactory == null) {
            this.durableDataLogFactory = new PermanentDurableDataLogFactory(executorService());
        }

        val sf = this.storageFactory;
        val ddlf = this.durableDataLogFactory;
        return ServiceBuilder.newInMemoryBuilder(builderConfig)
                             .withStorageFactory(setup -> new ListenableStorageFactory(sf, storage::set))
                             .withDataLogFactory(setup -> ddlf);
    }

    private static class PermanentDurableDataLogFactory extends InMemoryDurableDataLogFactory {
        PermanentDurableDataLogFactory(ScheduledExecutorService executorService) {
            super(executorService);
        }

        @Override
        public void close() {
            // This method intentionally left blank; we want this factory to live between multiple recovery attempts.
        }
    }
}
