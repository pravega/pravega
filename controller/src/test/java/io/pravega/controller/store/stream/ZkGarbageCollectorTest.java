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

import static org.junit.Assert.assertEquals;

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
        Duration gcPeriod = Duration.ofSeconds(2);
        Duration delta = Duration.ofMillis(100);

        BlockingQueue<CompletableFuture<Void>> queue = new LinkedBlockingQueue<>();
        // A supplier that takes a future from the queue and returns it. 
        Supplier<CompletableFuture<Void>> gcwork = () -> Exceptions.handleInterrupted(queue::take);

        // create first gc. Wait until it becomes the leader.
        ZKGarbageCollector gc1 = new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod);
        awaitStart(gc1);
        assertEquals(0, gc1.getLatestBatch());

        // now create additional gcs. 
        ZKGarbageCollector gc2 = new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod);
        awaitStart(gc2);
        assertEquals(0, gc2.getLatestBatch());

        // verify that only gc1's gcwork is called because it is the leader. gc2 is never called
        Futures.delayedFuture(gcPeriod.plus(delta), executor).join();

        gc1.fetchVersion().join();
        gc2.fetchVersion().join();

        assertEquals(1, gc1.getLatestBatch());
        assertEquals(1, gc2.getLatestBatch());

        // now post a failed future in the queue. 
        queue.add(Futures.failedFuture(new RuntimeException()));

        // the processing should not fail and gc should happen in the next period. 
        Futures.delayedFuture(gcPeriod.plus(delta), executor).join();
        // at least one of the three GC will be able to take the guard and run the periodic processing.
        // add some delay
        assertEquals(2, gc1.getLatestBatch());
        assertEquals(2, gc2.getLatestBatch());

        queue.add(CompletableFuture.completedFuture(null));

        // now stop GC1 so that gc2 become leader for GC workflow.
        gc1.stopAsync();
        gc1.awaitTerminated();

        Futures.delayedFuture(gcPeriod.plus(delta), executor).join();
        gc2.fetchVersion().join();
        assertEquals(3, gc2.getLatestBatch());
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
