/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.periodic;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;

import java.util.concurrent.Executor;

public class InMemoryStoreRetentionTest extends StreamCutServiceTest {

    @Override
    protected StreamMetadataStore createStore(int bucketCount, Executor executor) {
        return StreamStoreFactory.createInMemoryStore(bucketCount, executor);
    }
}
