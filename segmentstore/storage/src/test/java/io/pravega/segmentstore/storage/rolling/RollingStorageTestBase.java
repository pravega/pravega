/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.SyncStorage;

/**
 * Base class for testing any Storage implementation that has a layer of RollingStorage.
 */
public abstract class RollingStorageTestBase extends StorageTestBase {
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);

    @Override
    public void testFencing() throws Exception {
        // Fencing is left up to the underlying Storage implementation to handle. There's nothing to test here.
    }

    @Override
    protected void createSegment(String segmentName, Storage storage) {
        storage.create(segmentName, new SegmentRollingPolicy(getSegmentRollingSize()), null).join();
    }

    protected Storage wrap(SyncStorage storage) {
        return new AsyncStorageWrapper(new RollingStorage(storage, new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());
    }

    protected long getSegmentRollingSize() {
        return DEFAULT_ROLLING_SIZE;
    }
}
