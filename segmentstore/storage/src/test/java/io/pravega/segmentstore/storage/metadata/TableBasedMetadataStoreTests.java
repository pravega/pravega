/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.segmentstore.storage.mocks.InMemoryTableStore;
import lombok.val;
import org.junit.Before;

import java.util.concurrent.Executors;

/**
 *  Note that this is just a test for key-value store. Here the storage is NOT using this implementation.
 */
public class TableBasedMetadataStoreTests extends ChunkMetadataStoreTests {
    @Before
    public void setUp() throws Exception {
        val tableStore = new InMemoryTableStore(Executors.newScheduledThreadPool(10));
        metadataStore = new TableBasedMetadataStore("TEST", tableStore);
    }
}
