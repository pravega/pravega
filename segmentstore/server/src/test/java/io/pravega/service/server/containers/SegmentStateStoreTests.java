/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.containers;

import io.pravega.common.util.AsyncMap;
import io.pravega.service.storage.mocks.InMemoryStorage;
import lombok.val;

/**
 * Unit tests for the SegmentStateStore class.
 */
public class SegmentStateStoreTests extends StateStoreTests {
    @Override
    public int getThreadPoolSize() {
        return 5;
    }

    @Override
    protected AsyncMap<String, SegmentState> createStateStore() {
        val storage = new InMemoryStorage(executorService());
        storage.initialize(1);
        return new SegmentStateStore(storage, executorService());
    }
}
