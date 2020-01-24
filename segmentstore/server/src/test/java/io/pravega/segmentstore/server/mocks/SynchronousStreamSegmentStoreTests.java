/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return super.createBuilder(builderConfig, instanceId)
                    .withStreamSegmentStore(setup -> {
                        StreamSegmentStore base = new StreamSegmentService(setup.getContainerRegistry(), setup.getSegmentToContainerMapper());
                        return new SynchronousStreamSegmentStore(base);
                    });
    }
}
