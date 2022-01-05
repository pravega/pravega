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

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZkGarbageCollectorTest extends ThreadPooledTestSuite {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
    }

    @After
    public void cleanupTaskStore() throws IOException {
        cli.close();
        zkServer.close();
    }
    
    @Test(timeout = 30000)
    public void testGC() {
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executorService());
        String gcName = "testGC";
        Duration gcPeriod = Duration.ofSeconds(2);
        Duration delta = Duration.ofMillis(100);

        BlockingQueue<CompletableFuture<Void>> queue = new LinkedBlockingQueue<>();
        // A supplier that takes a future from the queue and returns it. 
        Supplier<CompletableFuture<Void>> gcwork = () -> Exceptions.handleInterruptedCall(queue::take);

        // create first gc. Wait until it becomes the leader.
        ZKGarbageCollector gc1 = new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod);
        awaitStart(gc1);
        assertEquals(0, gc1.getLatestBatch());

        // now create additional gcs. 
        ZKGarbageCollector gc2 = new ZKGarbageCollector(gcName, zkStoreHelper, gcwork, gcPeriod);
        awaitStart(gc2);
        assertEquals(0, gc2.getLatestBatch());

        // verify that only gc1's gcwork is called because it is the leader. gc2 is never called
        Futures.delayedFuture(gcPeriod.plus(delta), executorService()).join();

        gc1.fetchVersion().join();
        gc2.fetchVersion().join();

        assertEquals(1, gc1.getLatestBatch());
        assertEquals(1, gc2.getLatestBatch());

        // now post a failed future in the queue. 
        queue.add(Futures.failedFuture(new RuntimeException()));

        // the processing should not fail and gc should happen in the next period. 
        Futures.delayedFuture(gcPeriod.plus(delta), executorService()).join();
        // at least one of the three GC will be able to take the guard and run the periodic processing.
        // add some delay
        assertEquals(2, gc1.getLatestBatch());
        assertEquals(2, gc2.getLatestBatch());
    
        // fail processing with store connection exception
        queue.add(Futures.failedFuture(StoreException.create(StoreException.Type.CONNECTION_ERROR, "store connection")));

        // the processing should not fail and gc should happen in the next period. 
        Futures.delayedFuture(gcPeriod.plus(delta), executorService()).join();
        assertEquals(3, gc1.getLatestBatch());
        assertEquals(3, gc2.getLatestBatch());
        queue.add(CompletableFuture.completedFuture(null));

        // now stop GC1 so that gc2 become leader for GC workflow.
        gc1.stopAsync();
        gc1.awaitTerminated();

        Futures.delayedFuture(gcPeriod.plus(delta), executorService()).join();
        gc2.fetchVersion().join();
        assertEquals(4, gc2.getLatestBatch());
        
        // now deliberately set the gc version for gc2 to an older value and call process. 
        gc2.setVersion(0);
        gc2.process().join();
        
        assertEquals(4, gc2.getVersion());
        
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
        gc.addListener(listener, executorService());
        runningLatch.join();
    }
}
