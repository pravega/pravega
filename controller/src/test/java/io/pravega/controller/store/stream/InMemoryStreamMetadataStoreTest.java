/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryStreamMetadataStoreTest extends StreamMetadataStoreTest {

    @Override
    public void setupStore() throws Exception {
        store = StreamStoreFactory.createInMemoryStore(executor);
        bucketStore = StreamStoreFactory.createInMemoryBucketStore(1);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
    }
}
