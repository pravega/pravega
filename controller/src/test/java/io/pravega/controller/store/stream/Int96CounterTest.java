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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

/**
 * Int96 counter tests.
 */

public abstract class Int96CounterTest {
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

    @Test(timeout = 30000)
    public void testCounter() throws Exception {
        OperationContext context = new TestOperationContext();

        Int96Counter int96Counter = spy(getInt96Counter());

        // first call should get the new range from store
        Int96 counter = int96Counter.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(getCounterForTesting(int96Counter), counter);
        Int96 limit = getLimitForTesting(int96Counter);
        assertEquals(ZkInt96Counter.COUNTER_RANGE, limit.getLsb());

        // update the local counter to the end of the current range (limit - 1)
        setCounterAndLimitForTesting(limit.getMsb(), limit.getLsb() - 1, limit.getMsb(), limit.getLsb(), int96Counter);
        // now call three getNextCounters concurrently.. first one to execute should increment the counter to limit.
        // other two will result in refresh being called.
        CompletableFuture<Int96> future1 = int96Counter.getNextCounter(context);
        CompletableFuture<Int96> future2 = int96Counter.getNextCounter(context);
        CompletableFuture<Int96> future3 = int96Counter.getNextCounter(context);

        List<Int96> values = Futures.allOfWithResults(Arrays.asList(future1, future2, future3)).join();

        // second and third should result in refresh being called. Verify method call count is 3, twice for now and
        // once for first time when counter is set
        verifyRefreshRangeIfNeededCall(int96Counter, context);

        verifyGetRefreshFutureCall(int96Counter, context);

        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(limit.getMsb(), limit.getLsb())) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 1)) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 2)) == 0));

        // verify that counter and limits are increased
        Int96 newCounter = getCounterForTesting(int96Counter);
        Int96 newLimit = getLimitForTesting(int96Counter);
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, newLimit.getLsb());
        assertEquals(0, newLimit.getMsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE + 2, newCounter.getLsb());
        assertEquals(0, newCounter.getMsb());

        // set range in store to have lsb = Long.Max - 100
        mockCounterValue();
        // set local limit to {msb, Long.Max - 100}
        setCounterAndLimitForTesting(0, Long.MAX_VALUE - 100, 0, Long.MAX_VALUE - 100, int96Counter);
        // now the call to getNextCounter should result in another refresh
        int96Counter.getNextCounter(context).join();
        // verify that post refresh counter and limit have different msb
        Int96 newCounter2 = getCounterForTesting(int96Counter);
        Int96 newLimit2 = getLimitForTesting(int96Counter);

        assertEquals(1, newLimit2.getMsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE - 100, newLimit2.getLsb());
        assertEquals(0, newCounter2.getMsb());
        assertEquals(Long.MAX_VALUE - 99, newCounter2.getLsb());
    }

    @Test(timeout = 30000)
    public void testCounterConcurrentUpdates() {
        OperationContext context = new TestOperationContext();

        Int96Counter counter1 = spy(getInt96Counter());
        Int96Counter counter2 = spy(getInt96Counter());
        Int96Counter counter3 = spy(getInt96Counter());

        // first call should get the new range from store
        Int96 counter = counter1.getNextCounter(context).join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(getCounterForTesting(counter1), counter);
        Int96 limit = getLimitForTesting(counter1);
        assertEquals(ZkInt96Counter.COUNTER_RANGE, limit.getLsb());

        getRefreshFuture(counter3, context).join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE, getCounterForTesting(counter3).getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, getLimitForTesting(counter3).getLsb());

        getRefreshFuture(counter2, context).join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, getCounterForTesting(counter2).getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 3, getLimitForTesting(counter2).getLsb());

        getRefreshFuture(counter1, context).join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 3, getCounterForTesting(counter1).getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 4, getLimitForTesting(counter1).getLsb());
    }

    @Before
    public abstract void setupStore() throws Exception;

    @After
    public abstract void cleanupStore() throws Exception;

    abstract Int96Counter getInt96Counter();

    abstract Int96 getCounterForTesting(Int96Counter int96Counter);

    abstract Int96 getLimitForTesting(Int96Counter int96Counter);

    abstract CompletableFuture<Void> getRefreshFuture(Int96Counter int96Counter, OperationContext context);

    abstract void setCounterAndLimitForTesting(int counterMsb, long counterLsb, int limitMsb, long limitLsb,
                                               Int96Counter counter);

    abstract void mockCounterValue();

    abstract void verifyRefreshRangeIfNeededCall(Int96Counter int96Counter, OperationContext context);

    abstract void verifyGetRefreshFutureCall(Int96Counter int96Counter, OperationContext context);

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }
}
