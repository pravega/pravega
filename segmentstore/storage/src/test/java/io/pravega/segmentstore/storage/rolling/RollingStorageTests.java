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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.util.ArrayList;
import lombok.val;

/**
 * Unit tests for the RollingStorage class.
 */
public class RollingStorageTests extends StorageTestBase {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);

    // TODO: more unit test for RollingStorage particularities: actual rolling, small/failed concats, truncate, etc.
    // TODO: figure out what to do with TruncateableStorageTestBase. That won't work on this since it's not accurate.

    @Override
    public void testFencing() throws Exception {
        // Fencing is left up to the underlying Storage implementation to handle. There's nothing to test here.
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new RollingStorage(new InMemoryStorage(), DEFAULT_ROLLING_POLICY), executorService());
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        val headerName = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        val headerHandle = InMemoryStorage.newHandle(headerName, readOnly);
        return new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());
    }
}
