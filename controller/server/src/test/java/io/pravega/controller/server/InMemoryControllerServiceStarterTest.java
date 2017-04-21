/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.server;

import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;

/**
 * In-memory store based ControllerServiceStarter tests.
 */
public class InMemoryControllerServiceStarterTest extends ControllerServiceStarterTest {
    public InMemoryControllerServiceStarterTest() {
        super(true);
    }

    @Override
    public void setup() {
        storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
        storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
    }

    @Override
    public void tearDown() {
    }
}
