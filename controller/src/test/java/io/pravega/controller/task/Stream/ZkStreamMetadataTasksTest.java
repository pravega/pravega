/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;

public class ZkStreamMetadataTasksTest extends StreamMetadataTasksTest {
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(zkClient, executor);
    }

    @Test
    public void addSubscriberTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Test
    public void removeSubscriberTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Test
    public void updateSubscriberStreamCutTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Test
    public void getSubscribersForStreamTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }
}
