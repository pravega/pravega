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
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link CacheUtilizationProvider} class.
 */
public class CacheUtilizationProviderTests {
    private static final double COMPARE_ERROR = 0.001;
    private static final CachePolicy POLICY = new CachePolicy(1000, 0.7, 0.9, Duration.ofMillis(1000), Duration.ofMillis(100));

    /**
     * Tests the constructor with cache policy.
     */
    @Test
    public void testCachePolicy() {
        val cup = new CacheUtilizationProvider(POLICY, () -> 2L);
        Assert.assertEquals(POLICY.getTargetUtilization(), cup.getCacheTargetUtilization(), COMPARE_ERROR);
        Assert.assertEquals(POLICY.getMaxUtilization(), cup.getCacheMaxUtilization(), COMPARE_ERROR);
    }

    /**
     * Tests the {@link CacheUtilizationProvider#getCacheUtilization()} method.
     */
    @Test
    public void testCacheUtilization() {
        val usedBytes = new AtomicLong();
        val cup = new CacheUtilizationProvider(POLICY, usedBytes::get);
        Assert.assertEquals(0.0, cup.getCacheUtilization(), COMPARE_ERROR);
        usedBytes.set(100);
        Assert.assertEquals(0.1, cup.getCacheUtilization(), COMPARE_ERROR);
        cup.adjustPendingBytes(250);
        Assert.assertEquals(0.35, cup.getCacheUtilization(), COMPARE_ERROR);
        cup.adjustPendingBytes(-150);
        Assert.assertEquals(0.2, cup.getCacheUtilization(), COMPARE_ERROR);
        Assert.assertEquals(100, cup.getPendingBytes());
    }

    @Test
    public void testCacheInsertionCapacity() {
        val minThreshold = POLICY.getMaxUtilization() - 2 * (POLICY.getMaxUtilization() - POLICY.getTargetUtilization());
        val initialSize = (long) (POLICY.getMaxSize() * minThreshold);
        val maxUtilizationSize = (long) (POLICY.getMaxSize() * POLICY.getMaxUtilization());

        val usedBytes = new AtomicLong();
        val cup = new CacheUtilizationProvider(POLICY, usedBytes::get);

        // These value should yield the max utilization.
        Assert.assertEquals(1.0, cup.getCacheInsertionCapacity(), COMPARE_ERROR);
        usedBytes.set(initialSize);
        Assert.assertEquals(1.0, cup.getCacheInsertionCapacity(), COMPARE_ERROR);
        usedBytes.addAndGet(-10);
        cup.adjustPendingBytes(10);
        Assert.assertEquals(1.0, cup.getCacheInsertionCapacity(), COMPARE_ERROR);

        // These values should gradually reduce it to 0.
        double last = 1.0;
        for (long i = initialSize; i < maxUtilizationSize; i++) {
            if (i % 2 == 0) {
                usedBytes.incrementAndGet();
            } else {
                cup.adjustPendingBytes(1);
            }
            double current = cup.getCacheInsertionCapacity();
            Assert.assertTrue("Expected cache insertion capacity to decrease.", current < last);
            last = current;
        }

        // These values should yield 0.
        cup.adjustPendingBytes(1);
        Assert.assertEquals(0.0, cup.getCacheInsertionCapacity(), COMPARE_ERROR);
        cup.adjustPendingBytes(-1);
        usedBytes.incrementAndGet();
        Assert.assertEquals(0.0, cup.getCacheInsertionCapacity(), COMPARE_ERROR);
    }

    /**
     * Tests the {@link CacheUtilizationProvider#registerCleanupListener} and {@link CacheUtilizationProvider#notifyCleanupListeners}
     */
    @Test
    public void testCleanupListeners() {
        val usedBytes = new AtomicLong();
        val cup = new CacheUtilizationProvider(POLICY, usedBytes::get);
        TestCleanupListener l1 = new TestCleanupListener();
        TestCleanupListener l2 = new TestCleanupListener();
        cup.registerCleanupListener(l1);
        cup.registerCleanupListener(l2);
        cup.notifyCleanupListeners();
        Assert.assertEquals("Expected cleanup listener to be invoked the first time.", 1, l1.getCallCount());
        Assert.assertEquals("Expected cleanup listener to be invoked the first time.", 1, l2.getCallCount());

        l2.setClosed(true);
        cup.notifyCleanupListeners();
        Assert.assertEquals("Expected cleanup listener to be invoked the second time.", 2, l1.getCallCount());
        Assert.assertEquals("Not expected cleanup listener to be invoked the second time.", 1, l2.getCallCount());
        cup.registerCleanupListener(l2); // This should have no effect.

        // Now verify with errors.
        cup.registerCleanupListener(new ThrottleSourceListener() {
            @Override
            public void notifyThrottleSourceChanged() {
                throw new IntentionalException();
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });

        cup.notifyCleanupListeners();
        Assert.assertEquals("Expected cleanup listener to be invoked the third time.", 3, l1.getCallCount());
        Assert.assertEquals("Not expected cleanup listener to be invoked the third time.", 1, l2.getCallCount());
    }

    private static class TestCleanupListener implements ThrottleSourceListener {
        @Getter
        private int callCount = 0;
        @Setter
        @Getter
        private boolean closed;

        @Override
        public void notifyThrottleSourceChanged() {
            this.callCount++;
        }
    }
}
