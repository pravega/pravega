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
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;


/**
 * Int96 counter tests.
 */

public abstract class Int96CounterTest {
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
    protected ZKStoreHelper zkStoreHelper;
    protected CuratorFramework cli;
    private TestingServer zkServer;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        zkStoreHelper = spy(new ZKStoreHelper(cli, executor));
    }

    @Test(timeout = 30000)
    public void testCounter() {
        setupStore();
        OperationContext context = new TestOperationContext();
        Int96Counter int96Counter = spy(getInt96Counter());

        // first call should get the new range from store
        Int96 counter = int96Counter.getNextCounter(context).join();
        Int96CounterImpl int96CounterImpl = (Int96CounterImpl) int96Counter;
        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(int96CounterImpl.getCounterForTesting(), counter);
        Int96 limit = int96CounterImpl.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE, limit.getLsb());

        // update the local counter to the end of the current range (limit - 1)
        int96CounterImpl.setCounterAndLimitForTesting(limit.getMsb(), limit.getLsb() - 1, limit.getMsb(), limit.getLsb());
        // now call three getNextCounters concurrently.. first one to execute should increment the counter to limit.
        // other two will result in refresh being called.
        CompletableFuture<Int96> future1 = int96Counter.getNextCounter(context);
        CompletableFuture<Int96> future2 = int96Counter.getNextCounter(context);
        CompletableFuture<Int96> future3 = int96Counter.getNextCounter(context);

        List<Int96> values = Futures.allOfWithResults(Arrays.asList(future1, future2, future3)).join();

        // second and third should result in refresh being called. Verify method call count is 3, twice for now and
        // once for first time when counter is set
        verify(int96CounterImpl, times(3)).refreshRangeIfNeeded(context);
        verifyStoreCall();
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(limit.getMsb(), limit.getLsb())) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 1)) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 2)) == 0));

        // verify that counter and limits are increased
        Int96 newCounter = int96CounterImpl.getCounterForTesting();
        Int96 newLimit = int96CounterImpl.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE * 2, newLimit.getLsb());
        assertEquals(0, newLimit.getMsb());
        assertEquals(Int96Counter.COUNTER_RANGE + 2, newCounter.getLsb());
        assertEquals(0, newCounter.getMsb());

        // set range in store to have lsb = Long.Max - 100
        mockCounterValue();
        // set local limit to {msb, Long.Max - 100}
        int96CounterImpl.setCounterAndLimitForTesting(0, Long.MAX_VALUE - 100, 0, Long.MAX_VALUE - 100);
        // now the call to getNextCounter should result in another refresh
        int96Counter.getNextCounter(context).join();
        // verify that post refresh counter and limit have different msb
        Int96 newCounter2 = int96CounterImpl.getCounterForTesting();
        Int96 newLimit2 = int96CounterImpl.getLimitForTesting();

        assertEquals(1, newLimit2.getMsb());
        assertEquals(Int96Counter.COUNTER_RANGE - 100, newLimit2.getLsb());
        assertEquals(0, newCounter2.getMsb());
        assertEquals(Long.MAX_VALUE - 99, newCounter2.getLsb());

        int96CounterImpl.setCounterAndLimitForTesting(1, Long.MAX_VALUE - 100, 1, Long.MAX_VALUE - 100);
        assertThrows(CompletionException.class, () -> int96CounterImpl.getNextCounter(context).join());
    }

    protected abstract void verifyStoreCall();

    @Test(timeout = 30000)
    public void testCounterConcurrentUpdates() {
        setupStore();
        OperationContext context = new TestOperationContext();

        Int96Counter counter1 = spy(getInt96Counter());
        Int96Counter counter2 = spy(getInt96Counter());
        Int96Counter counter3 = spy(getInt96Counter());

        Int96CounterImpl abstractCounter1 = (Int96CounterImpl) counter1;
        Int96CounterImpl abstractCounter2 = (Int96CounterImpl) counter2;
        Int96CounterImpl abstractCounter3 = (Int96CounterImpl) counter3;

        // first call should get the new range from store
        Int96 counter = counter1.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(abstractCounter1.getCounterForTesting(), counter);
        Int96 limit = abstractCounter1.getLimitForTesting();
        assertEquals(Int96Counter.COUNTER_RANGE, limit.getLsb());

        abstractCounter3.function.apply(new Int96CounterImpl.CounterInfo(context, Int96Counter.COUNTER_RANGE),
                abstractCounter3::reset).join();
        assertEquals(Int96Counter.COUNTER_RANGE, abstractCounter3.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 2, abstractCounter3.getLimitForTesting().getLsb());

        abstractCounter2.function.apply(new Int96CounterImpl.CounterInfo(context, Int96Counter.COUNTER_RANGE),
                abstractCounter2::reset).join();
        assertEquals(Int96Counter.COUNTER_RANGE * 2, abstractCounter2.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 3, abstractCounter2.getLimitForTesting().getLsb());

        abstractCounter1.function.apply(new Int96CounterImpl.CounterInfo(context, Int96Counter.COUNTER_RANGE),
                abstractCounter1::reset).join();
        assertEquals(Int96Counter.COUNTER_RANGE * 3, abstractCounter1.getCounterForTesting().getLsb());
        assertEquals(Int96Counter.COUNTER_RANGE * 4, abstractCounter1.getLimitForTesting().getLsb());
    }

    public abstract void setupStore();

    abstract Int96Counter getInt96Counter();

    abstract void mockCounterValue();

    @After
    public void tearDown() throws Exception {
        cli.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }
}
