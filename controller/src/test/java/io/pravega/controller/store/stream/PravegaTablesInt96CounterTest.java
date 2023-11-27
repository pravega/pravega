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


import io.pravega.common.lang.Int96;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import static io.pravega.shared.NameUtils.TRANSACTION_ID_COUNTER_TABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * PravegaTables based counter tests.
 */

public class PravegaTablesInt96CounterTest extends Int96CounterTest {
    private PravegaTablesStoreHelper storeHelper;

    @Override
    public void setupStore() {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, authHelper, executor));
    }



    @Override
    Int96Counter getInt96Counter() {
        return spy(new PravegaTablesInt96Counter(storeHelper, zkStoreHelper));
    }

    @Override
    void mockCounterValue() {
        // set range in store to have lsb = Long.Max - 100
        VersionedMetadata<Int96> data = new VersionedMetadata<>(new Int96(0, Long.MAX_VALUE - 100),
                new Version.LongVersion(2L));
        doReturn(CompletableFuture.completedFuture(data)).when(storeHelper).getEntry(eq(TRANSACTION_ID_COUNTER_TABLE),
                eq(PravegaTablesInt96Counter.COUNTER_KEY), any(), anyLong());
    }

    @Test(timeout = 30000)
    public void testUpgradeScenario() {
        // Test to verify when we upgrade Pravega having txn counter into zookeeper. It should start Pravega tables store
        // txn counter from same value.
        setupStore();
        OperationContext context = new TestOperationContext();
        AbstractInt96Counter zkcounter1 = spy(new ZkInt96Counter(zkStoreHelper));
        zkStoreHelper.createZNodeIfNotExist("/store/scope").join();
        // first call should get the new range from store
        Int96 counter = zkcounter1.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(zkcounter1.getCounterForTesting(), counter);
        Int96 limit = zkcounter1.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE, limit.getLsb());

        Int96Counter tableCounter = spy(getInt96Counter());
        AbstractInt96Counter abstractCounter1 = (AbstractInt96Counter) tableCounter;
        Int96 newCounter = tableCounter.getNextCounter(context).join();
        Int96 tableLimit = abstractCounter1.getLimitForTesting();
        assertEquals(0, newCounter.compareTo(limit.add(1)));
        assertEquals(0, Int96.ZERO.add(2 * Int96Counter.COUNTER_RANGE).compareTo(tableLimit));

    }

}