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

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ZkGarbageCollectorTest {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private ScheduledExecutorService executor;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        executor = Executors.newScheduledThreadPool(2);
    }

    @After
    public void cleanupTaskStore() throws IOException {
        cli.close();
        zkServer.close();
        executor.shutdown();
    }
    
    @Test(timeout = 30000)
    public void testGC() {
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executor);
        String gcName = "testGC";
        Duration gcPeriod = Duration.ofSeconds(1);

        BlockingQueue<CompletableFuture<Void>> queue = new LinkedBlockingQueue<>();
        // A supplier that takes a future from the queue and returns it. 
        Supplier<CompletableFuture<Void>> gcwork1 = () -> Exceptions.handleInterrupted(queue::take);
        Supplier<CompletableFuture<Void>> gcwork = () -> CompletableFuture.completedFuture(null);
        
        // create first gc. Wait until it becomes the leader.  
        ZKGarbageCollector gc1 = spy(new ZKGarbageCollector(gcName, zkStoreHelper, gcwork1, gcPeriod));
        awaitStart(gc1);
        Futures.await(gc1.getAcquiredLeadership());
        
        // now create additional gcs. 
        ZKGarbageCollector gc2 = spy(new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod));
        awaitStart(gc2);
        assertFalse(gc2.getAcquiredLeadership().isDone());

        ZKGarbageCollector gc3 = spy(new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod));
        awaitStart(gc3);
        assertFalse(gc3.getAcquiredLeadership().isDone());

        long batch1 = gc1.getLatestBatch();
        long batch2 = gc2.getLatestBatch();
        long batch3 = gc3.getLatestBatch();
        assertTrue(batch1 == batch2 && batch1 == batch3);
        
        // verify that only gc1's gcwork is called periodically. gc2 and gc3 are never called
        Futures.delayedFuture(gcPeriod.multipliedBy(2), executor).join();
        verify(gc1, times(1)).process();
        verify(gc2, never()).process();
        verify(gc3, never()).process();
        
        // now post a failed future in the queue. 
        queue.add(Futures.failedFuture(new RuntimeException()));

        // the processing should not fail and gc should happen in the next period. 
        Futures.delayedFuture(gcPeriod.multipliedBy(2), executor).join();
        verify(gc1, times(2)).process();
        verify(gc2, never()).process();
        verify(gc3, never()).process();

        long newBatch1 = gc1.getLatestBatch();
        long newBatch2 = gc2.getLatestBatch();
        long newBatch3 = gc3.getLatestBatch();
        assertTrue(newBatch1 == newBatch2 && newBatch1 == newBatch3);
        assertTrue(newBatch1 > batch1);

        queue.add(CompletableFuture.completedFuture(null));
    }

    private void awaitStart(ZKGarbageCollector gc) {
        gc.startAsync();
        CompletableFuture<Void> runningLatch = new CompletableFuture<>();
        // Note: adding a listener because await running on a spied Abstract service is not working. 
        Service.Listener listener = new Service.Listener() {
            @Override
            public void running() {
                super.running();
                runningLatch.complete(null);
            }
        };
        gc.addListener(listener, executor);
        runningLatch.join();
    }
}
