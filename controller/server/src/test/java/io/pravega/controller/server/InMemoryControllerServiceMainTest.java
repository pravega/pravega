/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server;

import io.pravega.controller.store.client.impl.StoreClientConfigImpl;

/**
 * In-memory store based ControllerServiceMain tests.
 */
public class InMemoryControllerServiceMainTest extends ControllerServiceMainTest {
    public InMemoryControllerServiceMainTest() {
        super(true);
    }

    @Override
    public void setup() {
        storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
    }

    @Override
    public void tearDown() {
    }
}
