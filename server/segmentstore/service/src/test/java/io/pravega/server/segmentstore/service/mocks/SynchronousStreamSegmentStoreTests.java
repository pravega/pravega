/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service.mocks;

import io.pravega.server.segmentstore.contracts.StreamSegmentStore;
import io.pravega.server.segmentstore.service.store.ServiceBuilder;
import io.pravega.server.segmentstore.service.store.ServiceBuilderConfig;
import io.pravega.server.segmentstore.service.store.StreamSegmentService;
import io.pravega.server.segmentstore.service.store.StreamSegmentServiceTests;
import io.pravega.server.segmentstore.storage.Storage;

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
