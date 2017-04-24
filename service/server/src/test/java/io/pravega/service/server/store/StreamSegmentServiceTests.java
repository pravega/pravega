/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
