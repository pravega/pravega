/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import java.io.IOException;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryStreamMetadataStoreTest extends StreamMetadataStoreTest {

    @Override
    public void setupTaskStore() throws Exception {
        store = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
    }
}
