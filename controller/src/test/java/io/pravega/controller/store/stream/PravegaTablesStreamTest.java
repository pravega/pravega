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
package io.pravega.controller.store.stream;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.time.Duration;

public class PravegaTablesStreamTest extends StreamTestBase {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private PravegaTablesStreamMetadataStore store;
    private PravegaTablesStoreHelper storeHelper;
    private ZkOrderedStore orderer;
    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        orderer = new ZkOrderedStore("txnOrderer", new ZKStoreHelper(cli, executor), executor);
        store = new PravegaTablesStreamMetadataStore(segmentHelper, cli, executor, Duration.ofSeconds(1), authHelper);
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        cli.close();
        zkServer.close();
        executor.shutdown();
    }

    @Override
    void createScope(String scope) {
        store.createScope(scope).join();
    }

    @Override
    PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize) {
        PravegaTablesScope pravegaTableScope = new PravegaTablesScope(scope, storeHelper);
        pravegaTableScope.addStreamToScope(stream).join();
        
        return new PravegaTablesStream(scope, stream, storeHelper, orderer,  
                () -> 0, chunkSize, shardSize, pravegaTableScope::getStreamsInScopeTableName, executor);
    }
}
