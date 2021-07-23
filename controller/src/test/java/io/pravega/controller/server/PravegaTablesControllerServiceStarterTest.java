/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
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
                GrpcAuthHelper.getDisabledAuthHelper(), (CuratorFramework) storeClient.getClient(), executor);
    }

    @Override
    KVTableMetadataStore getKVTStore(StoreClient storeClient) {
        return KVTableStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                GrpcAuthHelper.getDisabledAuthHelper(), (CuratorFramework) storeClient.getClient(), executor);
    }
}
