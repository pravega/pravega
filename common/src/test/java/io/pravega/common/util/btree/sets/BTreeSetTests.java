/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree.sets;

import io.pravega.common.MathHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BTreeSet} class.
 */
public class BTreeSetTests extends ThreadPooledTestSuite {
    private static final Comparator<ArrayView> COMPARATOR = BTreeSet.COMPARATOR;
    private static final int MAX_ITEM_LENGTH = 256;
    private static final int MAX_PAGE_SIZE = 10000;
    private static final int MAX_ITEM_COUNT = 10000;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    /**
     * Upper bound on BTreeSetPage overhead.
     */
    private static final Function<Integer, Integer> GET_PAGE_OVERHEAD = itemCount -> 18 + itemCount * Integer.BYTES;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method sequentially (only with insertions)
     * making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitSequential() {
        val config = new InsertConfig();
        config.setMaxItemCount(Integer.MAX_VALUE);
        config.setExpectedFinalPageCount(1);
        config.setAllowDuplicates(true);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method using bulk-loading
     * (only with insertions) making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitBulk() {
        val config = new InsertConfig();
        config.setMaxItemCount(Integer.MAX_VALUE);
        config.setBulk(true);
        config.setExpectedFinalPageCount(1);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method sequentially (only with
     * insertions, in sorted order).
     */
    @Test
    public void testInsertSortedSequential() {
        val config = new InsertConfig();
        config.setMaxPageSize(Integer.MAX_VALUE);
        config.setSorted(true);
        config.setAllowDuplicates(true);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method sequentially (only with
     * insertions, in unsorted order).
     */
    @Test
    public void testInsertRandomSequential() {
        val config = new InsertConfig();
        config.setMaxPageSize(Integer.MAX_VALUE);
        config.setAllowDuplicates(true);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method in bulk order (only with
     * insertions, in sorted order).
     */
    @Test
    public void testInsertSortedBulk() {
        val config = new InsertConfig();
        config.setMaxPageSize(Integer.MAX_VALUE);
        config.setSorted(true);
        config.setBulk(true);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method in bulk order (only with
     * insertions, in unsorted order).
     */
    @Test
    public void testInsertRandomBulk() {
        val config = new InsertConfig();
        config.setMaxPageSize(Integer.MAX_VALUE);
        config.setBulk(true);
        testInsert(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method sequentially (with both
     * insertions and deletions).
     */
    @Test
    public void testRemoveSequential() {
        val config = new DeleteConfig();
        config.setMaxItemCount(1000);
        config.setDeleteBatchSize(1);
        testDelete(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method in bulk order (with both
     * insertions and deletions).
     */
    @Test
    public void testRemoveBulk() {
        val config = new DeleteConfig();
        config.setDeleteBatchSize(123);
        testDelete(config);
    }

    /**
     * Tests the {@link BTreeSet#update(Collection, Collection, Supplier, Duration)} method with removing all items at once.
     */
    @Test
    public void testRemoveAll() {
        val config = new DeleteConfig();
        config.setDeleteBatchSize(config.getMaxItemCount());
        testDelete(config);
    }

    /**
     * Tests the ability to process both updates and deletions at the same time.
     */
    @Test
    public void testInsertDelete() {
        val iterationCount = 100;
        val maxBatchSize = 2000;
        val maxUpdateSize = 32 * 1024 * 1024; // Just a sanity check that we don't generate too much data.
        val random = new Random(0);
        val ds = new DataSource();
        val set = newBTreeSet(ds);
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        for (int iterationId = 0; iterationId < iterationCount; iterationId++) {
            int insertBatchSize = random.nextInt(maxBatchSize); // This may be 0 - which is fine.

            val config = new GenerateConfig();
            config.setMaxItemCount(insertBatchSize);
            config.setMaxPageSize(maxUpdateSize);
            config.setAllowDuplicates(false);
            val toInsert = generate(config, random);
            val uniqueInserts = new HashSet<>(toInsert);

            val removeBatchSize = random.nextInt(maxBatchSize);
            val toRemove = pickRandomly(expectedItems, (double) removeBatchSize / maxBatchSize, random)
                    .stream()
                    .filter(a -> !uniqueInserts.contains(a))
                    .collect(Collectors.toList());

            set.update(toInsert, toRemove, ds::getNextPageId, TIMEOUT).join();
            expectedItems.removeAll(toRemove);
            expectedItems.addAll(toInsert);

            check(set, new ArrayList<>(expectedItems), 0, ds);
        }

        // Clear out the index.
        set.update(null, expectedItems, ds::getNextPageId, TIMEOUT).join();
        check(set, Collections.emptyList(), 0, ds);
        Assert.assertEquals("Unexpected final page count.", 1, ds.getPageCount());

        // Now insert a few more items and verify it works.
        val gc2 = new GenerateConfig();
        gc2.setMaxItemCount(100);
        gc2.setAllowDuplicates(false);
        expectedItems.clear();
        expectedItems.addAll(generate(gc2));
        set.update(expectedItems, null, ds::getNextPageId, TIMEOUT).join();
        check(set, new ArrayList<>(expectedItems), 0, ds);
    }

    private Collection<ArrayView> pickRandomly(TreeSet<ArrayView> existingItems, double probability, Random random) {
        return existingItems.stream()
                .filter(i -> random.nextDouble() <= probability)
                .collect(Collectors.toList());
    }

    /**
     * Tests the ability to iterate through items using {@link BTreeSet#iterator}.
     * Note: this only tests bounded iterators. Boundless (no first, no last) iterators are tested in {@link #check}.
     */
    @Test
    public void testIterator() {
        val ds = new DataSource();
        val set = newBTreeSet(ds);
        val config = new GenerateConfig();
        config.setAllowDuplicates(false);
        config.setMaxItemCount(1000);
        config.setMaxPageSize(Integer.MAX_VALUE);

        // Populate the tree.
        val items = generate(config);
        items.sort(COMPARATOR);
        set.update(items, null, ds::getNextPageId, TIMEOUT).join();

        // Check with valid arguments.
        for (int i = 0; i < items.size() / 2; i += 3) {
            val lastItemIndex = items.size() - i;
            val firstItem = items.get(i);
            val lastItem = items.get(lastItemIndex - 1);
            val expectedInclusive = items.subList(i, lastItemIndex);
            val inclusiveResult = collect(set.iterator(firstItem, true, lastItem, true, TIMEOUT));
            AssertExtensions.assertListEquals("", expectedInclusive, inclusiveResult, this::itemEquals);

            val expectedExclusive = items.subList(i + 1, lastItemIndex - 1);
            val exclusiveResult = collect(set.iterator(firstItem, false, lastItem, false, TIMEOUT));
            AssertExtensions.assertListEquals("", expectedExclusive, exclusiveResult, this::itemEquals);
        }

        // Check with illegal args.
        AssertExtensions.assertThrows(
                "Inclusive iterator accepted args out of order.",
                () -> set.iterator(items.get(1), true, items.get(0), true, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "Exclusive iterator accepted args out of order.",
                () -> set.iterator(items.get(0), true, items.get(0), false, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the behavior of the {@link BTreeSet} when there are data source write errors.
     */
    @Test
    public void testWriteErrors() {
    }

    private void testInsert(InsertConfig config) {
        val ds = new DataSource();
        val set = newBTreeSet(ds);

        List<ArrayView> items = generate(config);
        if (config.sorted) {
            items.sort(COMPARATOR);
        }

        if (config.bulk) {
            // BTreeSet.update disallows duplicates - we must deduplicate while preserving the sorted property.
            set.update(items, null, ds::getNextPageId, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of calls to persist for bulk-write.", 1, ds.getPersistCount());
        } else {
            int lastPageCount = ds.getPageCount();
            int expectedPersistCount = 0;
            for (val e : items) {
                set.update(Collections.singleton(e), null, ds::getNextPageId, TIMEOUT).join();
                expectedPersistCount++;
                Assert.assertEquals("Expected exactly one persist count per update.", expectedPersistCount, ds.getPersistCount());
                AssertExtensions.assertGreaterThanOrEqual("Not expecting number of pages to decrease.", lastPageCount, ds.getPageCount());
                lastPageCount = ds.getPageCount();
            }
        }

        if (config.getExpectedFinalPageCount() >= 0) {
            Assert.assertEquals("Unexpected page count.", config.getExpectedFinalPageCount(), ds.getPageCount());
        }

        // Verify index.
        check(set, items, 0, ds);

        // As opposed from BTreeIndex, we do not need to verify "recovery" as the BTreeSet does not keep any internal state.
    }

    private void testDelete(DeleteConfig config) {
        final int checkEvery = config.getMaxItemCount() / 10; // checking is very expensive; we don't want to do it every time.

        val ds = new DataSource();
        val set = newBTreeSet(ds);
        val items = generate(config);
        set.update(items, null, ds::getNextPageId, TIMEOUT).join();

        // When we delete we do not expect new pages to be generated.
        Supplier<Long> noNewPageSupplier = () -> {
            Assert.fail("Not expecting any new pages.");
            return -1L;
        };

        int firstIndex = 0;
        int lastPageCount = ds.getPageCount();
        int lastCheck = -1;
        while (firstIndex < items.size()) {
            int batchSize = Math.min(config.getDeleteBatchSize(), items.size() - firstIndex);
            val toDelete = items.subList(firstIndex, firstIndex + batchSize);

            set.update(null, toDelete, noNewPageSupplier, TIMEOUT).join();

            // Verify we are not actually storing new pages.
            AssertExtensions.assertLessThanOrEqual("Not expecting number of pages to increase.", lastPageCount, ds.getPageCount());
            lastPageCount = ds.getPageCount();

            // Determine if it's time to check the index.
            if (firstIndex - lastCheck > checkEvery) {
                // Search for all entries, and make sure only the ones we care about are still there.
                check(set, items, firstIndex + batchSize, ds);
                int itemCount = getItemCount(set);
                Assert.assertEquals("Unexpected item count after deleting " + (firstIndex + 1),
                        items.size() - firstIndex - batchSize, itemCount);
                lastCheck = firstIndex;
            }

            firstIndex += batchSize;
        }

        // Verify again, now that we have an empty index.
        check(set, Collections.emptyList(), 0, ds);
        val finalKeyCount = getItemCount(set);
        Assert.assertEquals("Not expecting any keys after deleting everything.", 0, finalKeyCount);

        // Verify that we only have one page now (the root).
        Assert.assertEquals("Unexpected final page count.", 1, ds.getPageCount());
    }

    private void check(BTreeSet set, List<ArrayView> items, int firstValidEntryIndex, DataSource ds) {
        val initialPersistCount = ds.getPersistCount();
        val initialReadCount = ds.getReadCount();
        val actualItems = collect(set.iterator(null, true, null, true, TIMEOUT));
        Assert.assertEquals("Expected each page to be read exactly once.", initialReadCount + ds.getPageCount(), ds.getReadCount());
        Assert.assertEquals("Not expected any updates to be performed.", initialPersistCount, ds.getPersistCount());

        // Sort and deduplicate items. The BTreeSet does not store duplicates and returns items in order.
        val expectedItems = deduplicateOrderedItems(items.stream().sorted(COMPARATOR).iterator());

        // Bulk-get returns a list of values in the same order as the keys, so we need to match up on the indices.
        Assert.assertEquals("Unexpected item count.", expectedItems.size() - firstValidEntryIndex, actualItems.size());
        if (actualItems.isEmpty()) {
            // We've already performed all the checks we had to.
            return;
        }

        for (int i = 0; i < expectedItems.size(); i++) {
            val a = actualItems.get(i);
            if (i < firstValidEntryIndex) {
                Assert.assertNull("Not expecting a result for index " + i, a);
            } else {
                Assert.assertEquals("Value mismatch for item index " + i, 0, COMPARATOR.compare(expectedItems.get(i), a));
            }
        }
    }

    private List<ArrayView> collect(AsyncIterator<List<ArrayView>> iterator) {
        val items = new ArrayList<ArrayView>();
        iterator.forEachRemaining(items::addAll, executorService()).join();
        return items;
    }

    private int getItemCount(BTreeSet set) {
        val count = new AtomicInteger();
        set.iterator(null, true, null, true, TIMEOUT)
                .forEachRemaining(k -> count.addAndGet(k.size()), executorService()).join();
        return count.get();
    }

    private List<ArrayView> deduplicateOrderedItems(Iterator<ArrayView> items) {
        val result = new ArrayList<ArrayView>();
        ArrayView last = null;
        while (items.hasNext()) {
            val i = items.next();
            int c = last == null ? -1 : COMPARATOR.compare(last, i);
            if (c > 0) {
                Assert.fail("Test error: unsorted array at index " + result.size());
            } else if (c != 0) {
                result.add(i);
            }
            last = i;
        }
        return result;
    }

    private ArrayList<ArrayView> generate(GenerateConfig config) {
        return generate(config, new Random(0));
    }

    private ArrayList<ArrayView> generate(GenerateConfig config, Random random) {
        val uniqueItems = new TreeSet<ArrayView>(COMPARATOR);
        val result = new ArrayList<ArrayView>();
        int count = 0;
        int size = 0;
        while (count < config.getMaxItemCount() && size + GET_PAGE_OVERHEAD.apply(count) < config.getMaxPageSize()) {
            val maxItemLen = config.getMaxPageSize() - size - GET_PAGE_OVERHEAD.apply(count + 1);
            val data = new byte[MathHelpers.minMax(random.nextInt(MAX_ITEM_LENGTH), 1, maxItemLen)];
            random.nextBytes(data);
            val item = new ByteArraySegment(data);
            if (!config.isAllowDuplicates() && !uniqueItems.add(item)) {
                continue; // We generated this before and aren't allowed to include duplicates.
            }

            result.add(item);
            count++;
            size += data.length;
        }

        return result;
    }

    private boolean itemEquals(ArrayView item1, ArrayView item2) {
        return COMPARATOR.compare(item1, item2) == 0;
    }

    private BTreeSet newBTreeSet(DataSource ds) {
        return new BTreeSet(MAX_PAGE_SIZE, MAX_ITEM_LENGTH, ds::read, ds::persist, executorService(), "TestBTreeSet");
    }

    private class DataSource {
        private final Map<Long, ArrayView> data = new ConcurrentHashMap<>();
        private final AtomicLong nextId = new AtomicLong(0);
        private final AtomicInteger persistCount = new AtomicInteger();
        private final AtomicInteger readCount = new AtomicInteger();

        long getNextPageId() {
            return this.nextId.getAndIncrement();
        }

        int getPageCount() {
            return this.data.size();
        }

        int getPersistCount() {
            return this.persistCount.get();
        }

        int getReadCount() {
            return this.readCount.get();
        }

        CompletableFuture<Void> persist(List<Map.Entry<Long, ArrayView>> toUpdate, Collection<Long> toDelete, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                AssertExtensions.assertGreaterThan("No updates or removals provided.", 0, toUpdate.size() + toDelete.size());
                toUpdate.forEach(e -> this.data.put(e.getKey(), e.getValue()));
                toDelete.forEach(this.data::remove);
                this.persistCount.incrementAndGet();
            }, executorService());
        }

        CompletableFuture<ArrayView> read(long pageId, Duration timeout) {
            this.readCount.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> this.data.getOrDefault(pageId, null), executorService());
        }
    }

    @Getter
    @Setter
    private static class GenerateConfig {
        int maxItemCount = MAX_ITEM_COUNT;
        int maxPageSize = MAX_PAGE_SIZE;
        boolean allowDuplicates = false;
    }

    @Getter
    @Setter
    private static class InsertConfig extends GenerateConfig {
        boolean sorted = false;
        boolean bulk = false;
        int expectedFinalPageCount = -1;
    }

    @Getter
    @Setter
    private static class DeleteConfig extends GenerateConfig {
        int deleteBatchSize = 1;
    }
}
