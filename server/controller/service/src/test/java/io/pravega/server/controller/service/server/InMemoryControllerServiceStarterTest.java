/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server;

import io.pravega.server.controller.service.store.client.StoreClientFactory;
import io.pravega.server.controller.service.store.client.impl.StoreClientConfigImpl;

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
