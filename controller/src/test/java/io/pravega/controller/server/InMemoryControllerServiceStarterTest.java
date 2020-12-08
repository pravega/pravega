/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;

/**
 * In-memory store based ControllerServiceStarter tests.
 */
public class InMemoryControllerServiceStarterTest extends ControllerServiceStarterTest {

    public InMemoryControllerServiceStarterTest() {
        this(false);
    }

    InMemoryControllerServiceStarterTest(boolean auth) {
        super(true, auth);
    }

    @Override
    public void setup() {
        storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
        storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
    }

    @Override
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }
}
