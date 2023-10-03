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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Slf4j
public class StorageHealthTrackerTests extends ThreadPooledTestSuite {

    public static final int CONTAINER_ID = 42;

    @Test
    public void testDefault() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Call throttle
        checkThrottling(storageHealthTracker, delaySupplier, 0);
        checkState(storageHealthTracker, false, false, false, false, 0);

        // End iteration
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 0);
    }

    private void checkState(StorageHealthTracker storageHealthTracker,
                            boolean expectedStorageUnavailable, boolean expectedDegraded, boolean expectedStorageFull,
                            boolean expectedSafeMode, int expectedPercentLate) {
        Assert.assertEquals(expectedStorageUnavailable, storageHealthTracker.isStorageUnavailable());
        Assert.assertEquals(expectedDegraded, storageHealthTracker.isStorageDegraded());
        Assert.assertEquals(expectedStorageFull, storageHealthTracker.isStorageFull());
        Assert.assertEquals(expectedSafeMode, storageHealthTracker.isSafeMode());
        Assert.assertEquals(expectedPercentLate, storageHealthTracker.getLatePercentage());
    }

    @Test
    public void testDefaultNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        checkState(storageHealthTracker, false, false, false, false, 0);
        for (int n = 0; n < 3; n++) {
            // Start iteration
            checkState(storageHealthTracker, false, false, false, false, 0);

            // Call throttle
            checkThrottling(storageHealthTracker, delaySupplier, 0);

            checkState(storageHealthTracker, false, false, false, false, 0);
            TestUtils.addRequestStats(storageHealthTracker, 0, 5, 0, 0);
            // End iteration
            storageHealthTracker.calculateHealthStats();
            checkState(storageHealthTracker, false, false, false, false, 0);
        }
    }

    @Test
    public void testTransitionNormalSlowNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make slow.
        TestUtils.addRequestStats(storageHealthTracker, 0, 6, 4, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Start iteration 2
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 3);
    }

    @Test
    public void testTransitionNormalSlowDegradedNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make slow.
        TestUtils.addRequestStats(storageHealthTracker, 0, 6, 4, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Start iteration 2 - still unavailable
        checkState(storageHealthTracker, false, false, false, false, 40);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Make degraded
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 9, 0);
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 90);

        // Start iteration 3
        checkState(storageHealthTracker, false, true, false, false, 90);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, false, true, false, false, 90);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalSlowSlowNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make degraded.
        TestUtils.addRequestStats(storageHealthTracker, 0, 6, 4, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Start iteration 2

        checkState(storageHealthTracker, false, false, false, false, 40);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Make slow
        TestUtils.addRequestStats(storageHealthTracker, 0, 5, 5, 0);
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 50);

        // Start iteration 3

        checkState(storageHealthTracker, false, false, false, false, 50);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, false, false, false, false, 50);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalSlowUnavailableNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1

        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make slow.
        TestUtils.addRequestStats(storageHealthTracker, 0, 6, 4, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Start iteration 2

        checkState(storageHealthTracker, false, false, false, false, 40);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, false, false, false, 40);

        // Make unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Start iteration 3

        checkState(storageHealthTracker, true, false, false, false, 33);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalDegradedNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1

        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make degraded.
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 8, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true,  false, false, 80);

        // Start iteration 2

        checkState(storageHealthTracker, false, true, false, false, 80);

        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 3);
    }

    @Test
    public void testTransitionNormalDegradedDegradedNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1 - Degraded

        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make degraded.
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 8, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Start iteration 2

        checkState(storageHealthTracker, false, true, false, false, 80);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Make degraded again
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 9, 0);
        // No more degraded, but should still throttle
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 90);

        // Start iteration 3

        checkState(storageHealthTracker, false, true, false, false, 90);

        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, false, true, false, false, 90);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalDegradedSlowNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1

        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make degraded.
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 8, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Start iteration 2 - still unavailable

        checkState(storageHealthTracker, false, true, false, false, 80);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Make slow
        TestUtils.addRequestStats(storageHealthTracker, 0, 7, 3, 0);
        // No more degraded, but should still throttle
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 30);

        // Start iteration 3

        checkState(storageHealthTracker, false, false, false, false, 30);

        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, false, false, false, false, 30);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalDegradedUnavailableNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1

        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make degraded.
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 8, 0);

        // Should not yet throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 80);
        // Should now throttle
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Make unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        checkThrottling(storageHealthTracker, delaySupplier, 6);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 9);
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 9);
    }

    @Test
    public void testTransitionNormalUnavailableNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // At start it is normal and no throttle
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // Make unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 2, 1);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 40);

        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 6);
        // Make normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);
        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        checkThrottling(storageHealthTracker, delaySupplier, 6);
    }

    @Test
    public void testTransitionNormalUnavailableDegradedNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // At start it is normal and no throttle
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // Make unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 2, 1);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 40);

        // still unavailable should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 6);
        checkState(storageHealthTracker, true, false, false, false, 40);

        // Make degraded
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 8, 0);

        // Still throttle
        checkThrottling(storageHealthTracker, delaySupplier, 9);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, true, false, false, 80);

        // Start iteration 3 - Normal
        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 12);
        // Make normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);
        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        checkThrottling(storageHealthTracker, delaySupplier, 12);
    }

    @Test
    public void testTransitionNormalUnavailableSlowNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        // At start it is normal and no throttle
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // Make unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 2, 1);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true,  false, false, false, 40);

        // still unavailable should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 6);
        checkState(storageHealthTracker, true, false, false, false, 40);

        // Make slow
        TestUtils.addRequestStats(storageHealthTracker, 0, 7, 3, 0);

        // Still throttle
        checkThrottling(storageHealthTracker, delaySupplier, 9);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 30);

        // Start iteration 3 - Normal
        // Should still throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 12);
        // Make normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);
        // End iteration 3
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        checkThrottling(storageHealthTracker, delaySupplier, 12);
    }

    @Test
    public void testTransitionNormalUnavailableUnavailableNormal() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID, ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);

        // make unavailable.
        TestUtils.addRequestStats(storageHealthTracker, 0, 2, 2, 1);

        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 3);

        // End iteration 1
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 40);

        // Start iteration 2 - still unavailable
        checkThrottling(storageHealthTracker, delaySupplier, 6);
        checkState(storageHealthTracker, true, false, false, false, 40);

        // Still unavailable
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        checkThrottling(storageHealthTracker, delaySupplier, 9);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Start iteration 3 - Unavailable
        // Should throttle.
        checkThrottling(storageHealthTracker, delaySupplier, 12);
        checkState(storageHealthTracker, true, false, false, false, 33);

        // Make it normal.
        TestUtils.addRequestStats(storageHealthTracker, 0, 9, 1, 0);

        // End iteration 2
        storageHealthTracker.calculateHealthStats();
        checkState(storageHealthTracker, false, false, false, false, 10);
        // No throttling
        checkThrottling(storageHealthTracker, delaySupplier, 12);
    }

    private void checkThrottling(StorageHealthTracker storageHealthTracker, TestDelaySupplier delaySupplier, int wantedNumberOfInvocations) {
        // Call throttle
        storageHealthTracker.throttleGarbageCollectionBatch().join();
        storageHealthTracker.throttleParallelOperation().join();
        storageHealthTracker.throttleExclusiveOperation().join();

        verify(delaySupplier, times(wantedNumberOfInvocations)).apply(any());
    }

    @Test
    public void testStorageUsed() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .maxSafeStorageSize(1000)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);
        storageHealthTracker.setStorageUsed(100);
        Assert.assertEquals(100, storageHealthTracker.getStorageUsed());
        Assert.assertEquals(10, storageHealthTracker.getStorageUsedPercentage(), 0);
    }

    @Test
    public void testStorageFull() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .maxSafeStorageSize(1000)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);
        Assert.assertFalse(storageHealthTracker.isStorageFull());
        Assert.assertFalse(storageHealthTracker.isSafeMode());
        storageHealthTracker.setStorageFull(true);
        Assert.assertTrue(storageHealthTracker.isStorageFull());
        Assert.assertTrue(storageHealthTracker.isSafeMode());
        storageHealthTracker.setStorageFull(false);
        Assert.assertFalse(storageHealthTracker.isStorageFull());
        Assert.assertFalse(storageHealthTracker.isSafeMode());
    }

    @Test
    public void testStorageFullDisabled() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .maxSafeStorageSize(1000)
                        .safeStorageSizeCheckEnabled(false)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);
        Assert.assertFalse(storageHealthTracker.isStorageFull());
        Assert.assertFalse(storageHealthTracker.isSafeMode());
        storageHealthTracker.setStorageFull(true);
        Assert.assertFalse(storageHealthTracker.isStorageFull());
        Assert.assertFalse(storageHealthTracker.isSafeMode());
        storageHealthTracker.setStorageFull(false);
        Assert.assertFalse(storageHealthTracker.isStorageFull());
        Assert.assertFalse(storageHealthTracker.isSafeMode());
    }

    @Test
    public void testInvalidArgs() {
        AssertExtensions.assertThrows(
                " should throw an exception",
                () -> new StorageHealthTracker(0, null, System::currentTimeMillis, new TestDelaySupplier()),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw an exception",
                () -> new StorageHealthTracker(0, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, null, new TestDelaySupplier()),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw an exception",
                () -> new StorageHealthTracker(0, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, System::currentTimeMillis, null),
                ex -> ex instanceof NullPointerException);
    }

    @Test
    public void testThrottleDuration() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .minLateThrottleDurationInMillis(100)
                        .maxLateThrottleDurationInMillis(1100)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);
        verify(delaySupplier, times(0)).apply(Duration.ofMillis(100 + storageHealthTracker.getLatePercentage() * 1000));
        TestUtils.addRequestStats(storageHealthTracker, 0, 3, 7, 0);
        // End iteration 1
        storageHealthTracker.calculateHealthStats();

        // Start iteration 2
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        verify(delaySupplier, times(3)).apply(Duration.ofMillis(800)); // 70% throttle
        // End iteration 2
        storageHealthTracker.calculateHealthStats();
    }

    @Test
    public void testThrottleDurationForUnavailable() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .minLateThrottleDurationInMillis(100)
                        .maxLateThrottleDurationInMillis(1100)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);
        verify(delaySupplier, times(0)).apply(Duration.ofMillis(100 + storageHealthTracker.getLatePercentage() * 1000));
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        verify(delaySupplier, times(3)).apply(Duration.ofMillis(1100)); // Full throttle
        // End iteration 1
        storageHealthTracker.calculateHealthStats();
    }

    @Test
    public void testThrottleDurationForUnavailableMultipleIterations() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .minLateThrottleDurationInMillis(100)
                        .maxLateThrottleDurationInMillis(1100)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);
        verify(delaySupplier, times(0)).apply(Duration.ofMillis(100 + storageHealthTracker.getLatePercentage() * 1000));
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        // End iteration 1
        storageHealthTracker.calculateHealthStats();

        // Start iteration 2
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 1, 1);
        storageHealthTracker.calculateHealthStats();

        // Start iteration 3
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        verify(delaySupplier, times(3)).apply(Duration.ofMillis(2200));
        storageHealthTracker.calculateHealthStats();

        // Start iteration 3
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        verify(delaySupplier, times(3)).apply(Duration.ofMillis(2200));
    }

    @Test
    public void testThrottleDurationForDegraded() {
        val delaySupplier = spy(new TestDelaySupplier());
        val storageHealthTracker = new StorageHealthTracker(CONTAINER_ID,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .minLateThrottleDurationInMillis(100)
                        .maxLateThrottleDurationInMillis(1100)
                        .build(),
                System::currentTimeMillis,
                delaySupplier);

        // Initial
        checkState(storageHealthTracker, false, false, false, false, 0);

        // Start iteration 1
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 0);
        verify(delaySupplier, times(0)).apply(Duration.ofMillis(100 + storageHealthTracker.getLatePercentage() * 1000));
        TestUtils.addRequestStats(storageHealthTracker, 0, 1, 9, 0);
        // End iteration 1
        storageHealthTracker.calculateHealthStats();

        // Start iteration 2
        // No throttling.
        checkThrottling(storageHealthTracker, delaySupplier, 3);
        verify(delaySupplier, times(3)).apply(Duration.ofMillis(1100)); // Full throttle
        storageHealthTracker.calculateHealthStats();
    }

    /**
     * Delay supplier for the tests.
     */
    private static class TestDelaySupplier implements Function<Duration, CompletableFuture<Void>> {
        @Override
        public CompletableFuture<Void> apply(Duration duration) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
