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
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the SegmentStateStore class.
 */
public class SegmentStateStoreTests extends StateStoreTests {
    private Storage storage;

    @Before
    public void setUp() {
        this.storage = InMemoryStorageFactory.newStorage(executorService());
        this.storage.initialize(1);
    }

    @Override
    public int getThreadPoolSize() {
        return 5;
    }

    @Override
    protected AsyncMap<String, SegmentState> createStateStore() {
        return new SegmentStateStore(this.storage, executorService());
    }

    /**
     * Tests the get() method when there exists a state in Storage, however it is an empty file.
     */
    @Test
    public void testGetEmptyState() throws Exception {
        final String segmentName = "foo";
        final String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        val store = createStateStore();
        this.storage.create(stateSegment, TIMEOUT).join();
        val state = store.get(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any state for a segment with no state.", state);
    }

    /**
     * Tests the get() method when there exists a state in Storage, however it is a corrupted file.
     */
    @Test
    public void testGetCorruptedState() throws Exception {
        final String segmentName = "foo";
        final String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        val store = createStateStore();

        // Write some dummy contents in the file which is not how a SegmentState would be serialized.
        this.storage.create(stateSegment, TIMEOUT)
                    .thenCompose(si -> this.storage.openWrite(stateSegment))
                    .thenCompose(handle -> this.storage.write(handle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT))
                    .join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected behavior when attempting to read a corrupted state file.",
                () -> store.get(segmentName, TIMEOUT),
                ex -> ex instanceof DataCorruptionException);
    }
}
