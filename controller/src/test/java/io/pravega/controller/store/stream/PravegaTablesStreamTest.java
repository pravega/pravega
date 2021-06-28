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

import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.ZKStoreHelper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.junit.ClassRule;

import java.time.Duration;

public class PravegaTablesStreamTest extends StreamTestBase {

    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(8000, 5000, RETRY_POLICY);
    private PravegaTablesStreamMetadataStore store;
    private PravegaTablesStoreHelper storeHelper;
    private ZkOrderedStore orderer;
    @Override
    public void setup() throws Exception {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        orderer = new ZkOrderedStore("txnOrderer", new ZKStoreHelper(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor), executor);
        store = new PravegaTablesStreamMetadataStore(segmentHelper, PRAVEGA_ZK_CURATOR_RESOURCE.client, executor, Duration.ofSeconds(1), authHelper);
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        executor.shutdown();
    }

    @Override
    void createScope(String scope, OperationContext context) {
        store.createScope(scope, context, executor).join();
    }

    @Override
    PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize) {
        PravegaTablesScope pravegaTableScope = new PravegaTablesScope(scope, storeHelper);
        OperationContext context = getContext();
        pravegaTableScope.addStreamToScope(stream, context).join();
        
        return new PravegaTablesStream(scope, stream, storeHelper, orderer,  
                () -> 0, chunkSize, shardSize, pravegaTableScope::getStreamsInScopeTableName, executor);
    }
}
