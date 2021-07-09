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
package io.pravega.controller.task.KeyValueTable;

import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.junit.ClassRule;

public class PravegaTablesKVTMetadataTasksTest extends TableMetadataTasksTest {

    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(8000, 5000, RETRY_POLICY);

    private SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);

    @Override
    public void setupStores() throws Exception {
        SegmentHelper segmentHelperMockForTables = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        // setup Stream Store, needed for creating scopes
        this.streamStore = StreamStoreFactory.createPravegaTablesStore(segmentHelperMockForTables, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        // setup KVTable Store
        this.kvtStore = KVTableStoreFactory.createPravegaTablesStore(segmentHelperMockForTables, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }

    @Override
    public void cleanupStores() throws Exception {
        kvtStore.close();
        streamStore.close();
    }

    @Override
    SegmentHelper getSegmentHelper() {
        return this.segmentHelper;
    }
}
