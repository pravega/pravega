/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * ControllerServiceStarter backed by ZK store tests.
 */
@Slf4j
public class PravegaTablesControllerServiceStarterTest extends ZKBackedControllerServiceStarterTest {
    @Override
    StoreClientConfig getStoreConfig(ZKClientConfig zkClientConfig) {
        return StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);
    }

    @Override
    StreamMetadataStore getStore(StoreClient storeClient) {
        return StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor), 
                AuthHelper.getDisabledAuthHelper(), (CuratorFramework) storeClient.getClient(), executor);
    }
}
