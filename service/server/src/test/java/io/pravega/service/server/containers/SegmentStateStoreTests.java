/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
