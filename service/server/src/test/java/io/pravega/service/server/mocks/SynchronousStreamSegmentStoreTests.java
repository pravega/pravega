/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.mocks;

import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.service.server.store.StreamSegmentService;
import io.pravega.service.server.store.StreamSegmentServiceTests;
import io.pravega.service.storage.Storage;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the SynchronousStreamSegmentStore class.
 */
public class SynchronousStreamSegmentStoreTests extends StreamSegmentServiceTests {

    @Override
    protected int getThreadPoolSize() {
        // We await all async operations, which means we'll be eating up a lot of threads for this test.
        return super.getThreadPoolSize() * 10;
    }

    @Override
    protected synchronized ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig, AtomicReference<Storage> storage) {
        return super.createBuilder(builderConfig, storage)
                    .withStreamSegmentStore(setup -> {
                        StreamSegmentStore base = new StreamSegmentService(setup.getContainerRegistry(), setup.getSegmentToContainerMapper());
                        return new SynchronousStreamSegmentStore(base);
                    });
    }
}
