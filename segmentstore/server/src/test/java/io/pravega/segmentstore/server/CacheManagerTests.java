/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.cache.CacheSnapshot;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.cache.NoOpCache;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the CacheManager class.
 */
public class CacheManagerTests extends ThreadPooledTestSuite {
    private static final int CLEANUP_TIMEOUT_MILLIS = 2000;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability to increment the current generation (or not) based on the activity of the clients.
     */
    @Test
    public void testIncrementCurrentGeneration() {
        final int clientCount = 10;
        final int cycleCount = 12345;
        final CachePolicy policy = new CachePolicy(Integer.MAX_VALUE, Duration.ofHours(10000), Duration.ofHours(1));
        Random random = RandomFactory.create();

        @Cleanup
        val cache = new TestCache(policy.getMaxSize());
        cache.setStoredBytes(1); // The Cache Manager won't do anything if there's no stored data.
        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());

        // Register a number of clients
        ArrayList<TestClient> clients = new ArrayList<>();
        for (int i = 0; i < clientCount; i++) {
            TestClient c = new TestClient();
            clients.add(c);
            cm.register(c);
        }

        // Run through a number of cycles
        AtomicInteger currentGeneration = new AtomicInteger();
        for (int cycleId = 0; cycleId < cycleCount; cycleId++) {
            boolean activityInCycle = cycleId % 2 == 0;
            HashSet<TestClient> updatedClients = new HashSet<>();

            // Initially, declare each client as having no activity this cycle
            clients.forEach(c -> c.setCacheStatus(0, Math.max(0, currentGeneration.get() - 1)));

            if (activityInCycle) {
                // Active cycle: pick a single, random client that will declare it had activity in the last generation.
                clients.get(random.nextInt(clients.size())).setCacheStatus(0, currentGeneration.get());

                // Fail the test if we get an unexpected value for currentGeneration.
                clients.forEach(c ->
                        c.setUpdateGenerationsImpl((current, oldest) -> {
                            Assert.assertEquals("Unexpected value for current generation.", currentGeneration.get(), (int) current);
                            updatedClients.add(c);
                            return false;
                        }));

                // There was activity in this cycle, so increment the expected current generation so we match what the CacheManager s doing.
                currentGeneration.incrementAndGet();
            } else {
                // Non-active cycle: each client will declare that they had no activity in the last generation.
                clients.forEach(c ->
                        c.setUpdateGenerationsImpl((current, oldest) -> {
                            updatedClients.add(c);
                            return false;
                        }));
            }

            cm.applyCachePolicy();

            if (activityInCycle) {
                Assert.assertEquals("CacheManager did not update all Clients with generation information when activity did happen during the cycle.",
                        clients.size(), updatedClients.size());
            } else {
                Assert.assertEquals("CacheManager updated Generations when no activity happened during the cycle.",
                        0, updatedClients.size());
            }
        }
    }

    /**
     * Tests the ability to increment the oldest generation (or not) based on the activity of the clients.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testIncrementOldestGeneration() {
        final int cycleCount = 12345;
        final int defaultOldestGeneration = 0;
        final int maxSize = 2048;
        final double targetUtilization = 0.5;
        final double maxUtilization = 0.95;
        final CachePolicy policy = new CachePolicy(maxSize, targetUtilization, maxUtilization, Duration.ofHours(10 * cycleCount), Duration.ofHours(1));
        final long excess = policy.getMaxSize(); // This is the excess size when we want to test Oldest Generation increases.
        @Cleanup
        val cache = new TestCache(policy.getMaxSize());
        cache.setStoredBytes(1); // The Cache Manager won't do anything if there's no stored data.
        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());

        // Use a single client (we tested multiple clients with Newest Generation).
        TestClient client = new TestClient();
        cm.register(client);

        // First do a dry-run - we need this to make sure the current generation advances enough.
        AtomicInteger currentGeneration = new AtomicInteger();
        for (int cycleId = 0; cycleId < cycleCount * 3; cycleId++) {
            client.setCacheStatus(0, currentGeneration.get());
            client.setUpdateGenerationsImpl((current, oldest) -> false);
            cm.applyCachePolicy();
            currentGeneration.incrementAndGet();
        }

        // Run a number of iterations
        AtomicInteger currentOldestGeneration = new AtomicInteger(defaultOldestGeneration);
        for (int cycleId = 0; cycleId < cycleCount; cycleId++) {
            boolean exceeds = cycleId % 2 == 0;
            boolean smallReductions = exceeds && cycleId % 4 == 0;
            boolean smallReductionsButNoRepeat = smallReductions && cycleId % 8 == 0;
            AtomicInteger callCount = new AtomicInteger();
            if (exceeds) {
                // If the total size does exceed the policy limit, repeated calls to 'update' should be made until either
                // the cache is within limits or no change can be made.
                cache.setUsedBytes(policy.getEvictionThreshold() + excess);
                client.setCacheStatus(currentOldestGeneration.get(), currentGeneration.get());
                client.setUpdateGenerationsImpl((current, oldest) -> {
                    AssertExtensions.assertGreaterThan("Expected an increase in oldestGeneration.", currentOldestGeneration.get(), oldest);
                    currentOldestGeneration.set(oldest);
                    callCount.incrementAndGet();
                    if (smallReductionsButNoRepeat && callCount.get() > 0) {
                        return false;
                    } else {
                        long reduction = smallReductions ? excess / 2 : excess;
                        cache.setUsedBytes(cache.getUsedBytes() - reduction);
                        return true;
                    }
                });
            } else {
                // If the total size does not exceed the policy limit, nothing should change
                cache.setStoredBytes(policy.getEvictionThreshold() - 1);
                cache.setUsedBytes(policy.getEvictionThreshold() - 1);
                client.setCacheStatus(defaultOldestGeneration, currentGeneration.get());
                client.setUpdateGenerationsImpl((current, oldest) -> {
                    Assert.assertEquals("Not expecting a change for oldestGeneration", currentOldestGeneration.get(), (int) oldest);
                    return false;
                });
            }

            cm.applyCachePolicy();

            // Verify how many times updateGenerations was invoked.
            int expectedCallCount = 0;
            if (exceeds) {
                if (smallReductionsButNoRepeat) {
                    expectedCallCount = 1; // We purposefully returned 0 above in order to force no-repeats.
                } else if (smallReductions) {
                    expectedCallCount = 2; // This is derived by how we constructed the 'excess' variable.
                } else {
                    expectedCallCount = 1; // Upon the first reduction we chop away the entire value of 'excess', so we only expect 1.
                }
            }
            Assert.assertEquals(
                    String.format("Unexpected number of calls to Client.updateGenerations(). Cycle=%d,Exceeds=%s,SmallReductions=%s.", cycleId, exceeds, smallReductions),
                    expectedCallCount,
                    callCount.get());
        }
    }

    /**
     * Tests the ability of the CacheManager to auto-unregister a client that was detected as having been closed.
     */
    @Test
    public void testAutoUnregister() {
        final CachePolicy policy = new CachePolicy(1024, Duration.ofHours(1), Duration.ofHours(1));
        @Cleanup
        val cache = new TestCache(policy.getMaxSize());
        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());
        TestClient client = new TestClient();
        cm.register(client);

        // Setup the client so that it throws ObjectClosedException when updateGenerations is called.
        client.setCacheStatus(0, 0);
        client.setUpdateGenerationsImpl((current, oldest) -> {
            throw new ObjectClosedException(this);
        });
        cm.applyCachePolicy();

        // Now do the actual verification.
        client.setCacheStatus(0, 1);
        client.setUpdateGenerationsImpl((current, oldest) -> {
            Assert.fail("Client was not unregistered after throwing ObjectClosedException.");
            return false;
        });
        cm.applyCachePolicy();
    }

    /**
     * Tests the case where the CacheManager deals with clients that have no data in them.
     */
    @Test
    public void testEmptyClients() {
        final CachePolicy policy = new CachePolicy(1024, Duration.ofHours(1), Duration.ofHours(1));
        @Cleanup
        val cache = new TestCache(policy.getMaxSize());
        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());
        TestClient client = new TestClient();
        cm.register(client);

        // Setup the client so that it throws ObjectClosedException when updateGenerations is called.
        cache.setStoredBytes(policy.getMaxSize() + 1);
        cache.setUsedBytes(policy.getMaxSize() + 1);
        client.setCacheStatus(0, 0);
        client.setUpdateGenerationsImpl((current, oldest) -> {
            Assert.assertEquals("Expected current generation to change.", 1, (long) current);
            Assert.assertEquals("Expected oldest generation to change.", 1, (long) oldest);
            return true;
        });
        cm.applyCachePolicy();
        cache.setStoredBytes(0);
        cache.setUsedBytes(0);
        client.setCacheStatus(0, 0);
        client.setUpdateGenerationsImpl((current, oldest) -> {
            Assert.fail("Not expecting any updates in generations.");
            return false;
        });
        cm.applyCachePolicy();
    }

    /**
     * Tests the ability to auto-cleanup the cache if it indicates it has reached capacity and needs some eviction(s)
     * in order to accomodate more data.
     */
    @Test
    public void testCacheFullCleanup() {
        final CachePolicy policy = new CachePolicy(1024, Duration.ofHours(1), Duration.ofHours(1));
        @Cleanup
        val cache = new DirectMemoryCache(policy.getMaxSize());
        int maxCacheSize = (int) cache.getSnapshot().getMaxBytes();

        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());
        TestClient client = new TestClient();
        cm.register(client);

        // Almost fill up the cache.
        int length1 = maxCacheSize / 2;
        val write1 = cache.insert(new ByteArraySegment(new byte[length1]));

        // Setup the TestClient to evict write1 when requested.
        val cleanupRequestCount = new AtomicInteger(0);
        client.setCacheStatus(0, 1);
        client.setUpdateGenerationsImpl((ng, og) -> {
            cleanupRequestCount.incrementAndGet();
            cache.delete(write1);
            return true;
        });

        // Insert an entry that would fill up the cache.
        int length2 = maxCacheSize / 2 + 1;
        val write2 = cache.insert(new ByteArraySegment(new byte[length2]));

        // Verify we were asked to cleanup.
        Assert.assertEquals("Unexpected number of cleanup requests.", 1, cleanupRequestCount.get());
        Assert.assertEquals("New entry was not inserted.", length2, cache.get(write2).getLength());
        Assert.assertEquals("Unexpected number of stored bytes.", length2, cache.getSnapshot().getStoredBytes());
    }

    /**
     * Tests the ability to handle concurrent requests to {@link  CacheManager#applyCachePolicy()}.
     */
    @Test
    public void testApplyPolicyConcurrency() throws Exception {
        // Almost fill up the cache.
        final CachePolicy policy = new CachePolicy(1024, Duration.ofHours(1), Duration.ofHours(1));
        @Cleanup
        val cache = new DirectMemoryCache(policy.getMaxSize());
        int maxCacheSize = (int) cache.getSnapshot().getMaxBytes();

        @Cleanup
        TestCacheManager cm = new TestCacheManager(policy, cache, executorService());
        TestClient client = new TestClient();
        cm.register(client);

        // Almost fill up the cache (75%)
        int initialLength = maxCacheSize * 3 / 4;
        val initialWrite = cache.insert(new ByteArraySegment(new byte[initialLength]));

        // Setup the TestClient to evict write1 when requested.
        val firstCleanupRequested = new CompletableFuture<Void>();
        val firstCleanupBlock = new CompletableFuture<Void>();
        val cleanupRequestCount = new AtomicInteger(0);
        val concurrentRequest = new AtomicBoolean(false);
        client.setCacheStatus(0, 1);
        client.setUpdateGenerationsImpl((ng, og) -> {
            int rc = cleanupRequestCount.incrementAndGet();
            if (rc == 1) {
                // This is the first concurrent request requesting a cleanup.
                // Notify that cleanup has been requested.
                firstCleanupRequested.complete(null);

                // Wait until we are ready to proceed.
                firstCleanupBlock.join();

                // We only need to delete this once.
                cache.delete(initialWrite);
            } else {
                // This is the second concurrent request requesting a cleanup.
                if (!firstCleanupBlock.isDone()) {
                    // This has executed before the first reuqest completed.
                    concurrentRequest.set(true);
                }
            }

            return true;
        });

        // Send one write that would end up filling the cache.
        int length1 = maxCacheSize / 3;
        val write1Future = CompletableFuture.supplyAsync(() -> cache.insert(new ByteArraySegment(new byte[length1])), executorService());

        // Wait for the cleanup to be requested.
        firstCleanupRequested.get(CLEANUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Send another write that would also fill up the cache.
        int length2 = length1 + 1;
        val write2Future = CompletableFuture.supplyAsync(() -> cache.insert(new ByteArraySegment(new byte[length2])), executorService());

        Thread.sleep(50);

        // Unblock the first cleanup.
        firstCleanupBlock.complete(null);

        // Get the results of the two suspended writes.
        val write1 = write1Future.get(CLEANUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val write2 = write2Future.get(CLEANUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify that things did work as intended.
        Assert.assertFalse("Concurrent call to applyCachePolicy detected.", concurrentRequest.get());
        Assert.assertEquals("Unexpected number of cleanup requests", 2, cleanupRequestCount.get());
        Assert.assertEquals("Unexpected entry #2.", length1, cache.get(write1).getLength());
        Assert.assertEquals("Unexpected entry #3.", length2, cache.get(write2).getLength());
    }

    private static class TestClient implements CacheManager.Client {
        private CacheManager.CacheStatus currentStatus;
        private BiFunction<Integer, Integer, Boolean> updateGenerationsImpl = (current, oldest) -> false;

        void setCacheStatus(int oldestGeneration, int newestGeneration) {
            this.currentStatus = new CacheManager.CacheStatus(oldestGeneration, newestGeneration);
        }

        void setUpdateGenerationsImpl(BiFunction<Integer, Integer, Boolean> function) {
            this.updateGenerationsImpl = function;
        }

        @Override
        public CacheManager.CacheStatus getCacheStatus() {
            return this.currentStatus;
        }

        @Override
        public boolean updateGenerations(int currentGeneration, int oldestGeneration) {
            return this.updateGenerationsImpl.apply(currentGeneration, oldestGeneration);
        }
    }

    @RequiredArgsConstructor
    @Getter
    @Setter
    private static class TestCache extends NoOpCache {
        private long storedBytes;
        private long usedBytes;
        private final long maxBytes;

        @Override
        public CacheSnapshot getSnapshot() {
            val s = super.getSnapshot();
            return new CacheSnapshot(this.storedBytes, this.usedBytes, 0, 0, this.maxBytes);
        }
    }
}
