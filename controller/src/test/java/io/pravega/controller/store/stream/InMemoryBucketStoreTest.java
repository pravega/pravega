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

import com.google.common.collect.ImmutableMap;

public class InMemoryBucketStoreTest extends BucketStoreTest {
    @Override
    public BucketStore getBucketStore(ImmutableMap<BucketStore.ServiceType, Integer> map) {
        return new InMemoryBucketStore(map);
    }
}
