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


import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.NameUtils.TRANSACTION_ID_COUNTER_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class PravegaTablesInt96CounterTest {
    private ScheduledExecutorService executor;
    private SegmentHelper segmentHelper;
    private GrpcAuthHelper authHelper;
    private PravegaTablesStoreHelper storeHelper;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, authHelper, executor));
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testCounter() {
        OperationContext context = new TestOperationContext();
        PravegaTablesInt96Counter pravegaTablesInt96Counter = spy(new PravegaTablesInt96Counter(storeHelper));

        // first call should get the new range from store
        Int96 counter = pravegaTablesInt96Counter.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(pravegaTablesInt96Counter.getCounterForTesting(), counter);
        Int96 limit = pravegaTablesInt96Counter.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE, limit.getLsb());

        // update the local counter to the end of the current range (limit - 1)
        pravegaTablesInt96Counter.setCounterAndLimitForTesting(limit.getMsb(), limit.getLsb() - 1,
                limit.getMsb(), limit.getLsb());
        // now call three getNextCounters concurrently.. first one to execute should increment the counter to limit.
        // other two will result in refresh being called.
        CompletableFuture<Int96> future1 = pravegaTablesInt96Counter.getNextCounter(context);
        CompletableFuture<Int96> future2 = pravegaTablesInt96Counter.getNextCounter(context);
        CompletableFuture<Int96> future3 = pravegaTablesInt96Counter.getNextCounter(context);

        List<Int96> values = Futures.allOfWithResults(Arrays.asList(future1, future2, future3)).join();

        // second and third should result in refresh being called. Verify method call count is 3, twice for now and
        // once for first time when counter is set
        verify(pravegaTablesInt96Counter, times(3)).refreshRangeIfNeeded(context);

        verify(pravegaTablesInt96Counter, times(2)).getRefreshFuture(context);

        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(limit.getMsb(), limit.getLsb())) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 1)) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 2)) == 0));

        // verify that counter and limits are increased
        Int96 newCounter = pravegaTablesInt96Counter.getCounterForTesting();
        Int96 newLimit = pravegaTablesInt96Counter.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE * 2, newLimit.getLsb());
        assertEquals(0, newLimit.getMsb());
        assertEquals(Int96Counter.COUNTER_RANGE + 2, newCounter.getLsb());
        assertEquals(0, newCounter.getMsb());

        // set range in store to have lsb = Long.Max - 100
        VersionedMetadata<Int96> data = new VersionedMetadata<>(new Int96(0, Long.MAX_VALUE - 100),
                new Version.LongVersion(2L));
        doReturn(CompletableFuture.completedFuture(data)).when(storeHelper).getEntry(eq(TRANSACTION_ID_COUNTER_TABLE),
                eq(PravegaTablesInt96Counter.COUNTER_KEY), any(), anyLong());
        // set local limit to {msb, Long.Max - 100}
        pravegaTablesInt96Counter.setCounterAndLimitForTesting(0, Long.MAX_VALUE - 100, 0,
                Long.MAX_VALUE - 100);
        // now the call to getNextCounter should result in another refresh
        pravegaTablesInt96Counter.getNextCounter(context).join();
        // verify that post refresh counter and limit have different msb
        Int96 newCounter2 = pravegaTablesInt96Counter.getCounterForTesting();
        Int96 newLimit2 = pravegaTablesInt96Counter.getLimitForTesting();

        assertEquals(1, newLimit2.getMsb());
        assertEquals(Int96Counter.COUNTER_RANGE - 100, newLimit2.getLsb());
        assertEquals(0, newCounter2.getMsb());
        assertEquals(Long.MAX_VALUE - 99, newCounter2.getLsb());
    }

    @Test(timeout = 30000)
    public void testCounterConcurrentUpdates() {
        OperationContext context = new TestOperationContext();
        PravegaTablesInt96Counter counter1 = spy(new PravegaTablesInt96Counter(storeHelper));
        PravegaTablesInt96Counter counter2 = spy(new PravegaTablesInt96Counter(storeHelper));
        PravegaTablesInt96Counter counter3 = spy(new PravegaTablesInt96Counter(storeHelper));

        // first call should get the new range from store
        Int96 counter = counter1.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(counter1.getCounterForTesting(), counter);
        Int96 limit = counter1.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE, limit.getLsb());

        counter3.getRefreshFuture(context).join();
        assertEquals(Int96Counter.COUNTER_RANGE, counter3.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 2, counter3.getLimitForTesting().getLsb());

        counter2.getRefreshFuture(context).join();
        assertEquals(Int96Counter.COUNTER_RANGE * 2, counter2.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 3, counter2.getLimitForTesting().getLsb());

        counter1.getRefreshFuture(context).join();
        assertEquals(Int96Counter.COUNTER_RANGE * 3, counter1.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 4, counter1.getLimitForTesting().getLsb());
    }
}