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
package io.pravega.client.connection.impl;

import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class FlowToBatchSizeTrackerTest {

    @Test(timeout = 10000L)
    public void testGetAppendBatchSizeTrackerByFlowId() {
        FlowToBatchSizeTracker flowToBatchSizeTracker = new FlowToBatchSizeTracker();
        // First, check the behavior when no flow id is in the cache.
        AppendBatchSizeTracker tracker = flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(0);
        Assert.assertNotNull(tracker);
        int newTrackerBatchSize = tracker.getAppendBlockSize();
        Assert.assertEquals("Error in newly instantiated tracker", newTrackerBatchSize, 0);
        tracker.recordAppend(0, 1);
        tracker.recordAppend(1, 1);
        tracker.recordAppend(2, 1);
        Assert.assertNotEquals(newTrackerBatchSize, tracker.getAppendBlockSize());
        Assert.assertNotEquals(tracker, flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(1));
        Assert.assertEquals(tracker, flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(0));
    }

    @Test(timeout = 10000L)
    public void testFlowToBatchSizeTrackerEviction() throws InterruptedException {
        Duration evictionTime = Duration.ofSeconds(1);
        int cacheMaxSize = 10;
        CompletableFuture<Void> latch = new CompletableFuture<>();
        BiConsumer<Integer, AppendBatchSizeTracker> callback = (flow, tracker) -> latch.complete(null);
        FlowToBatchSizeTracker flowToBatchSizeTracker = new FlowToBatchSizeTracker(cacheMaxSize, evictionTime, callback);
        // First, add one entry in the flow cache.
        AppendBatchSizeTracker tracker = flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(0);
        Assert.assertNotNull(tracker);
        // Confirm that the entry is stored in the cache.
        Assert.assertEquals(tracker, flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(0));
        // Wait for the expiration time to complete.
        Thread.sleep(1000);
        //Add another entry and wait for the cache clean up to get triggered.
        flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(1);
        // Wait for the eviction timeout to complete.
        latch.join();
        // Check that, if we get the tracker for the same flow, the object is a new one (i.e., it has been evicted and re-created)
        Assert.assertNotEquals(tracker, flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(0));
        // Check that the size constraints on the map are enforced.
        for (int i = 2; i < 20; i++) {
            flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(i);
        }
        Assert.assertEquals(cacheMaxSize, flowToBatchSizeTracker.getFlowToBatchSizeTrackerMap().size());
    }

}
