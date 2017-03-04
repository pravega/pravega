/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;

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
        return new SegmentStateStore(new InMemoryStorage(executorService()), executorService());
    }
}
