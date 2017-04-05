package com.emc.pravega.controller.server;

import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;

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
