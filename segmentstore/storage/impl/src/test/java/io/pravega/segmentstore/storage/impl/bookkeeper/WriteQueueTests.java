/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.AbstractTimer;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the WriteQueue class.
 */
public class WriteQueueTests {
    private static final int MAX_PARALLELISM = 10;
    private static final int ITEM_COUNT = MAX_PARALLELISM * 10;

    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    /**
     * Tests the basic functionality of the add() method.
     */
    @Test
    public void testAdd() {
        final int timeIncrement = 1234 * 1000; // Just over 1ms.
        AtomicLong time = new AtomicLong();
        val q = new WriteQueue(MAX_PARALLELISM, time::get);
        val initialStats = q.getStatistics();
        Assert.assertEquals("Unexpected getSize on empty queue.", 0, initialStats.getSize());
        Assert.assertEquals("Unexpected getAverageFillRate on empty queue.", 0, initialStats.getAverageItemFillRate(), 0);
        Assert.assertEquals("Unexpected getExpectedProcessingTimeMillis on empty queue.", 0, initialStats.getExpectedProcessingTimeMillis());
        Assert.assertEquals("Unexpected getMaxParallelism on empty queue.", MAX_PARALLELISM, initialStats.getMaxParallelism());

        int expectedSize = 0;
        long firstItemTime = 0;
        for (int i = 0; i < ITEM_COUNT; i++) {
            time.addAndGet(timeIncrement);
            if (i == 0) {
                firstItemTime = time.get();
            }

            q.add(new Write(new ByteArraySegment(new byte[i]), new TestWriteLedger(i), CompletableFuture.completedFuture(null)));
            expectedSize += i;

            val stats = q.getStatistics();
            val expectedFillRate = (double) expectedSize / stats.getSize() / BookKeeperConfig.MAX_APPEND_LENGTH;
            val expectedProcTime = (time.get() - firstItemTime) / AbstractTimer.NANOS_TO_MILLIS;
            Assert.assertEquals("Unexpected getSize.", i + 1, stats.getSize());
            Assert.assertEquals("Unexpected getAverageFillRate.", expectedFillRate, stats.getAverageItemFillRate(), 0.01);
            Assert.assertEquals("Unexpected getExpectedProcessingTimeMillis.", expectedProcTime, stats.getExpectedProcessingTimeMillis());
            Assert.assertEquals("Unexpected getMaxParallelism", MAX_PARALLELISM, stats.getMaxParallelism());
        }
    }

    /**
     * Tests the close() method.
     */
    @Test
    public void testClose() {
        val q = new WriteQueue(MAX_PARALLELISM);

        val expectedWrites = new ArrayList<Write>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            val w = new Write(new ByteArraySegment(new byte[i]), new TestWriteLedger(i), CompletableFuture.completedFuture(null));
            q.add(w);
            expectedWrites.add(w);
        }

        val removedWrites = q.close();
        AssertExtensions.assertListEquals("Unexpected writes removed.", expectedWrites, removedWrites, Object::equals);

        val clearStats = q.getStatistics();
        Assert.assertEquals("Unexpected getSize after clear.", 0, clearStats.getSize());
        Assert.assertEquals("Unexpected getAverageFillRate after clear.", 0, clearStats.getAverageItemFillRate(), 0);
        Assert.assertEquals("Unexpected getExpectedProcessingTimeMillis after clear.", 0, clearStats.getExpectedProcessingTimeMillis());
        Assert.assertEquals("Unexpected getMaxParallelism after clear.", MAX_PARALLELISM, clearStats.getMaxParallelism());

        AssertExtensions.assertThrows(
                "add() worked after close().",
                () -> q.add(new Write(new ByteArraySegment(new byte[1]), new TestWriteLedger(0), CompletableFuture.completedFuture(null))),
                ex -> ex instanceof ObjectClosedException);
        AssertExtensions.assertThrows(
                "getWritesToExecute() worked after close().",
                () -> q.getWritesToExecute(1),
                ex -> ex instanceof ObjectClosedException);
        AssertExtensions.assertThrows(
                "removeFinishedWrites() worked after close().",
                q::removeFinishedWrites,
                ex -> ex instanceof ObjectClosedException);
    }

    /**
     * Tests the removeFinishedWrites() method.
     */
    @Test
    public void testRemoveFinishedWrites() {
        final int timeIncrement = 1234 * 1000; // Just over 1ms.
        AtomicLong time = new AtomicLong();
        val q = new WriteQueue(MAX_PARALLELISM, time::get);

        val writes = new ArrayDeque<Write>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            time.addAndGet(timeIncrement);
            val w = new Write(new ByteArraySegment(new byte[i]), new TestWriteLedger(i), new CompletableFuture<>());
            if (i % 2 == 0) {
                // Complete 1 out of two writes.
                w.setEntryId(i);
                w.complete();
            }

            q.add(w);
            writes.addLast(w);
        }

        while (!writes.isEmpty()) {
            val write = writes.pollFirst();
            if (!write.isDone()) {
                val result1 = q.removeFinishedWrites();
                AssertExtensions.assertContainsSameElements("Unexpected value from removeFinishedWrites when there were writes left in the queue.",
                        EnumSet.of(WriteQueue.CleanupStatus.QueueNotEmpty), result1);
                val stats1 = q.getStatistics();
                Assert.assertEquals("Unexpected size after removeFinishedWrites with no effect.", writes.size() + 1, stats1.getSize());

                // Complete this write.
                write.setEntryId(time.get());
                write.complete();
            }

            // Estimate the Expected elapsed time based on the removals.
            long expectedElapsed = write.getTimestamp();
            int removed = 1;
            while (!writes.isEmpty() && writes.peekFirst().isDone()) {
                expectedElapsed += writes.pollFirst().getTimestamp();
                removed++;
            }
            expectedElapsed = (time.get() * removed - expectedElapsed) / AbstractTimer.NANOS_TO_MILLIS / removed;

            val result2 = q.removeFinishedWrites();
            val expectedResult = EnumSet.of(writes.isEmpty() ? WriteQueue.CleanupStatus.QueueEmpty : WriteQueue.CleanupStatus.QueueNotEmpty);
            AssertExtensions.assertContainsSameElements("Unexpected result from removeFinishedWrites.", expectedResult, result2);
            val stats2 = q.getStatistics();
            Assert.assertEquals("Unexpected size after removeFinishedWrites.", writes.size(), stats2.getSize());
            Assert.assertEquals("Unexpected getExpectedProcessingTimeMillis after clear.", expectedElapsed, stats2.getExpectedProcessingTimeMillis());
        }

        // Verify that it does report failed writes when encountered.
        val w3 = new Write(new ByteArraySegment(new byte[1]), new TestWriteLedger(0), new CompletableFuture<>());
        q.add(w3);
        w3.fail(new IntentionalException(), true);
        val result3 = q.removeFinishedWrites();
        AssertExtensions.assertContainsSameElements("Unexpected value from removeFinishedWrites when there were failed writes.",
                EnumSet.of(WriteQueue.CleanupStatus.QueueEmpty, WriteQueue.CleanupStatus.WriteFailed), result3);

    }

    /**
     * Tests the getWritesToExecute() method.
     */
    @Test
    public void testGetWritesToExecute() {
        final int ledgerChangeIndex = ITEM_COUNT - MAX_PARALLELISM / 2;
        val q = new WriteQueue(MAX_PARALLELISM);

        val writes = new ArrayList<Write>();
        int ledgerId = 0;
        for (int i = 0; i < ITEM_COUNT; i++) {
            if (i == ledgerChangeIndex) {
                ledgerId++;
            }

            val w = new Write(new ByteArraySegment(new byte[i]), new TestWriteLedger(ledgerId), new CompletableFuture<>());
            q.add(w);
            writes.add(w);
        }

        // 1. Throttled
        val throttledResult = q.getWritesToExecute(Long.MAX_VALUE);
        AssertExtensions.assertListEquals("Unexpected writes fetched with count throttling.",
                writes.subList(0, MAX_PARALLELISM), throttledResult, Object::equals);

        // 2. Max size reached.
        int sizeLimit = 10;
        val maxSizeResult = q.getWritesToExecute(sizeLimit);
        val expectedMaxSizeResult = new ArrayList<Write>();
        for (Write w : writes) {
            if (w.data.getLength() > sizeLimit) {
                break;
            }
            sizeLimit -= w.data.getLength();
            expectedMaxSizeResult.add(w);
        }

        AssertExtensions.assertListEquals("Unexpected writes fetched with size limit.",
                expectedMaxSizeResult, maxSizeResult, Object::equals);

        //3. Complete a few writes, then mark a few as in progress.
        writes.get(0).setEntryId(0);
        writes.get(0).complete();
        writes.get(1).beginAttempt();
        val result1 = q.getWritesToExecute(Long.MAX_VALUE);

        // We expect to skip over the first one and second one, but count the second one when doing throttling.
        AssertExtensions.assertListEquals("Unexpected writes fetched when some writes in progress (at beginning).",
                writes.subList(2, 1 + MAX_PARALLELISM), result1, Object::equals);

        //4. Mark a few writes as in progress after a non-progress write.
        writes.get(3).beginAttempt();
        val result2 = q.getWritesToExecute(Long.MAX_VALUE);
        Assert.assertEquals("Unexpected writes fetched when in-progress writes exist after non-in-progress writes.",
                0, result2.size());

        //5. LedgerChange.
        int beginIndex = ledgerChangeIndex - MAX_PARALLELISM / 2;
        for (int i = 0; i < beginIndex; i++) {
            writes.get(i).setEntryId(i);
            writes.get(i).complete();
        }

        q.removeFinishedWrites();
        val result3 = q.getWritesToExecute(Long.MAX_VALUE);
        AssertExtensions.assertListEquals("Unexpected writes fetched when ledger changed.",
                writes.subList(beginIndex, ledgerChangeIndex), result3, Object::equals);

        result3.forEach(w -> w.setEntryId(0));
        result3.forEach(Write::complete);
        q.removeFinishedWrites();
        val result4 = q.getWritesToExecute(Long.MAX_VALUE);
        AssertExtensions.assertListEquals("Unexpected writes fetched from the end, after ledger changed.",
                writes.subList(ledgerChangeIndex, writes.size()), result4, Object::equals);
    }

    private static class TestWriteLedger extends WriteLedger {
        TestWriteLedger(int ledgerId) {
            super(null, new LedgerMetadata(ledgerId, ledgerId));
        }
    }
}
