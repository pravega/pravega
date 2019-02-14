/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doReturn;

/**
 * Zookeeper based stream metadata store tests.
 */
public class ZKCounterTest {
    private TestingServer zkServer;
    private CuratorFramework cli;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        executor = Executors.newScheduledThreadPool(3);
    }

    @After
    public void tearDown() throws Exception {
        cli.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testCounter() throws Exception {
        ZKStoreHelper storeHelper = spy(new ZKStoreHelper(cli, executor));
        storeHelper.createZNodeIfNotExist("/store/scope").join();

        ZkInt96Counter zkStore = spy(new ZkInt96Counter(storeHelper));

        // first call should get the new range from store
        Int96 counter = zkStore.getNextCounter().join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(zkStore.getCounterForTesting(), counter);
        Int96 limit = zkStore.getLimitForTesting();
        assertEquals(ZkInt96Counter.COUNTER_RANGE, limit.getLsb());

        // update the local counter to the end of the current range (limit - 1)
        zkStore.setCounterAndLimitForTesting(limit.getMsb(), limit.getLsb() - 1, limit.getMsb(), limit.getLsb());
        // now call three getNextCounters concurrently.. first one to execute should increment the counter to limit.
        // other two will result in refresh being called.
        CompletableFuture<Int96> future1 = zkStore.getNextCounter();
        CompletableFuture<Int96> future2 = zkStore.getNextCounter();
        CompletableFuture<Int96> future3 = zkStore.getNextCounter();

        List<Int96> values = Futures.allOfWithResults(Arrays.asList(future1, future2, future3)).join();

        // second and third should result in refresh being called. Verify method call count is 3, twice for now and
        // once for first time when counter is set
        verify(zkStore, times(3)).refreshRangeIfNeeded();

        verify(zkStore, times(2)).getRefreshFuture();

        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(limit.getMsb(), limit.getLsb())) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 1)) == 0));
        assertTrue(values.stream().anyMatch(x -> x.compareTo(new Int96(0, limit.getLsb() + 2)) == 0));

        // verify that counter and limits are increased
        Int96 newCounter = zkStore.getCounterForTesting();
        Int96 newLimit = zkStore.getLimitForTesting();
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, newLimit.getLsb());
        assertEquals(0, newLimit.getMsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE + 2, newCounter.getLsb());
        assertEquals(0, newCounter.getMsb());

        // set range in store to have lsb = Long.Max - 100
        Data data = new Data(new Int96(0, Long.MAX_VALUE - 100).toBytes(), null);
        doReturn(CompletableFuture.completedFuture(data)).when(storeHelper).getData(ZkInt96Counter.COUNTER_PATH);
        // set local limit to {msb, Long.Max - 100}
        zkStore.setCounterAndLimitForTesting(0, Long.MAX_VALUE - 100, 0, Long.MAX_VALUE - 100);
        // now the call to getNextCounter should result in another refresh
        zkStore.getNextCounter().join();
        // verify that post refresh counter and limit have different msb
        Int96 newCounter2 = zkStore.getCounterForTesting();
        Int96 newLimit2 = zkStore.getLimitForTesting();

        assertEquals(1, newLimit2.getMsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE - 100, newLimit2.getLsb());
        assertEquals(0, newCounter2.getMsb());
        assertEquals(Long.MAX_VALUE - 99, newCounter2.getLsb());
    }

    @Test
    public void testCounterConcurrentUpdates() {
        ZKStoreHelper storeHelper = spy(new ZKStoreHelper(cli, executor));
        storeHelper.createZNodeIfNotExist("/store/scope").join();

        ZkInt96Counter counter1 = spy(new ZkInt96Counter(storeHelper));
        ZkInt96Counter counter2 = spy(new ZkInt96Counter(storeHelper));
        ZkInt96Counter counter3 = spy(new ZkInt96Counter(storeHelper));

        // first call should get the new range from store
        Int96 counter = counter1.getNextCounter().join();

        // verify that the generated counter is from new range
        assertEquals(0, counter.getMsb());
        assertEquals(1L, counter.getLsb());
        assertEquals(counter1.getCounterForTesting(), counter);
        Int96 limit = counter1.getLimitForTesting();
        assertEquals(ZkInt96Counter.COUNTER_RANGE, limit.getLsb());

        counter3.getRefreshFuture().join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE, counter3.getCounterForTesting().getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, counter3.getLimitForTesting().getLsb());

        counter2.getRefreshFuture().join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 2, counter2.getCounterForTesting().getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 3, counter2.getLimitForTesting().getLsb());

        counter1.getRefreshFuture().join();
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 3, counter1.getCounterForTesting().getLsb());
        assertEquals(ZkInt96Counter.COUNTER_RANGE * 4, counter1.getLimitForTesting().getLsb());
    }
}
