/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;

/**
 * Unit tests for the SegmentStateStore class.
 */
public class SegmentStateStoreTests extends StateStoreTests {
    private InMemoryStorage storage;

    @Override
    public int getThreadPoolSize() {
        return 5;
    }

    @Override
    protected AsyncMap<String, SegmentState> createStateStore() {
        storage = new InMemoryStorage(executorService());
        storage.initialize(1);
        return new SegmentStateStore(storage, executorService());
    }

    @Override
    public void emptySegment(String segmentName) {
        String firstStateSegment = segmentName + "$state1";
        storage.openWrite(firstStateSegment)
               .thenApply(handle -> storage.delete(handle, null))
                .thenApply(v -> storage.create(firstStateSegment, null)).join();

    }
}
