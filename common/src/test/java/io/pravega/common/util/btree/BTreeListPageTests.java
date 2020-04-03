/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArrayComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BTreeListPage} class.
 */
public class BTreeListPageTests {
    private static final int DEFAULT_PAGE_ID = 2;
    private static final int DEFAULT_PARENT_PAGE_ID = 1;
    private static final BTreeListPage.PagePointer DEFAULT_PAGE_INFO = new BTreeListPage.PagePointer(null, DEFAULT_PAGE_ID);
    private static final int MAX_ITEM_LENGTH = 4096;
    private static final ByteArrayComparator COMPARATOR = new ByteArrayComparator();
    private final Random random = new Random(0);

    /**
     * Tests {@link BTreeListPage.LeafPage#update(List)} with insertions.
     */
    @Test
    public void testLeafPageInsert() {
        val count = 3000;
        val page = new BTreeListPage.LeafPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);
        int updateCount = 0;
        int batchSize = 1;
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR::compare);
        val insertedKeys = new ArrayList<ArrayView>();
        while (updateCount < count) {
            val updateBatch = createUpdateBatch(batchSize);
            updateBatch.forEach(i -> expectedItems.add(i.getItem()));

            // Add some random key that has already been inserted.
            if (insertedKeys.size() > 0) {
                updateBatch.add(new BTreeListPage.UpdateItem(insertedKeys.get(random.nextInt(insertedKeys.size())), false));
            }
            updateBatch.forEach(i -> insertedKeys.add(i.getItem()));

            // Apply the update and verify it.
            updateBatch.sort(BTreeListPage.UpdateItem::compareTo);
            page.update(updateBatch);
            check(page, expectedItems);

            updateCount += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeListPage.LeafPage#update(List)} with deletions.
     */
    @Test
    public void testLeafPageDelete() {
        val count = 2000;
        val page = new BTreeListPage.LeafPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);

        // Bulk-update the page.
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR::compare);
        val updateBatch = createUpdateBatch(count);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(BTreeListPage.UpdateItem::compareTo);
        page.update(updateBatch);
        check(page, expectedItems);

        // Start removing arbitrary items from the page.
        int batchSize = 1;
        val shuffledItems = new ArrayList<ArrayView>(expectedItems);
        Collections.shuffle(shuffledItems, random);
        int index = 0;
        while (!expectedItems.isEmpty()) {
            // Generate a removal batch.
            batchSize = Math.min(batchSize, shuffledItems.size() - index);
            val removeBatch = shuffledItems.subList(index, index + batchSize)
                    .stream().map(item -> new BTreeListPage.UpdateItem(item, true)).collect(Collectors.toList());
            removeBatch.forEach(u -> expectedItems.remove(u.getItem()));
            removeBatch.sort(BTreeListPage.UpdateItem::compareTo);

            // Apply it and verify the page.
            page.update(removeBatch);
            check(page, expectedItems);

            index += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeListPage.LeafPage#update(List)} with both insertions and deletions.
     */
    @Test
    public void testLeafPageUpdateDelete() {
        val iterationCount = 200;
        val page = new BTreeListPage.LeafPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);
        int iterationId = 0;
        int batchSize = 1;
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR::compare);

        while (iterationId < iterationCount) {
            int removalCount = random.nextInt(batchSize);
            val shuffledItems = new ArrayList<ArrayView>(expectedItems);
            Collections.shuffle(shuffledItems, random);

            // Add some updates
            val updateBatch = createUpdateBatch(batchSize - removalCount);

            // Remove some existing keys.
            int actualRemovalCount = Math.min(removalCount, shuffledItems.size());
            for (int i = 0; i < actualRemovalCount; i++) {
                updateBatch.add(new BTreeListPage.UpdateItem(shuffledItems.get(i), true));
            }

            // Remove some extra (not present) keys.
            for (int i = actualRemovalCount; i < removalCount; i++) {
                updateBatch.add(new BTreeListPage.UpdateItem(newItem(), true));
            }

            updateBatch.sort(BTreeListPage.UpdateItem::compareTo);
            updateBatch.forEach(i -> {
                if (i.isRemoval()) {
                    expectedItems.remove(i.getItem());
                } else {
                    expectedItems.add(i.getItem());
                }
            });

            // Apply the update and verify it.
            page.update(updateBatch);
            check(page, expectedItems);

            iterationId++;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeListPage.IndexPage#addChildren}.
     */
    @Test
    public void testIndexPageAddChildren() {
        val childPageCount = 1000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR::compare).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new BTreeListPage.PagePointer(allItems.get(i), i))
                .collect(Collectors.toList());

        val page = new BTreeListPage.IndexPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);
        Assert.assertNull(page.getChildPage(newItem(), 0));

        int updateCount = 0;
        int batchSize = 1;
        val addedPointers = new ArrayList<BTreeListPage.PagePointer>();
        while (updateCount < allPointers.size()) {
            // Generate an update batch.
            batchSize = Math.min(batchSize, allPointers.size() - updateCount);
            val p = allPointers.subList(updateCount, updateCount + batchSize);

            // Add it.
            page.addChildren(p);
            addedPointers.addAll(p);

            // And verify it.
            check(page, addedPointers, allItems);

            updateCount += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeListPage.IndexPage#removeChildren}.
     */
    @Test
    public void testIndexPageRemoveChildren() {
        val childPageCount = 1000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR::compare).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new BTreeListPage.PagePointer(allItems.get(i), i))
                .collect(Collectors.toList());

        // Create a new page and add all items to it.
        val page = new BTreeListPage.IndexPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);
        page.addChildren(allPointers);
        check(page, allPointers, allItems);

        List<BTreeListPage.PagePointer> shuffledPointers = new ArrayList<>(allPointers);
        Collections.shuffle(shuffledPointers);
        int batchSize = 1;
        while (!allPointers.isEmpty()) {
            // Generate a removal batch.
            batchSize = Math.min(batchSize, shuffledPointers.size());
            val batch = shuffledPointers.subList(0, batchSize);
            batch.sort((p1, p2) -> COMPARATOR.compare(p1.getKey(), p2.getKey()));

            // Apply it
            page.removeChildren(batch);
            shuffledPointers = shuffledPointers.subList(batchSize, shuffledPointers.size());

            // And verify it.
            allPointers.removeAll(batch);
            check(page, allPointers, allItems);

            batchSize++;
        }
    }

    /**
     * Tests the ability to split a Leaf Page.
     */
    @Test
    public void testLeafPageSplit() {
        val itemCount = 5000;
        val page = new BTreeListPage.LeafPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID);

        // Bulk-update the page.
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR::compare);
        val updateBatch = createUpdateBatch(itemCount);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(BTreeListPage.UpdateItem::compareTo);
        page.update(updateBatch);

        val nextPageId = new AtomicLong(1000);
        int maxPageSize = page.size();
        while (maxPageSize > MAX_ITEM_LENGTH) {
            val splitResult = page.split(maxPageSize, nextPageId::getAndIncrement);
            boolean requiresSplit = maxPageSize < page.size();
            if (requiresSplit) {
                // Verify counts match.
                val splitItemCount = splitResult.stream().mapToInt(BTreeListPage::getItemCount).sum();
                Assert.assertEquals(expectedItems.size(), splitItemCount);

                // We will iterate through all split page's items in order and compare them against our expected order.
                val expectedItemIterator = expectedItems.iterator();
                val pageIds = new HashSet<Long>();
                for (val s : splitResult) {
                    AssertExtensions.assertGreaterThan("Not expecting any empty split pages.", 0, s.size());
                    AssertExtensions.assertLessThanOrEqual("Split page too large.", maxPageSize, s.size());

                    // Verify page pointers are correct.
                    Assert.assertTrue("Duplicate Page Id.", pageIds.add(s.getPagePointer().getPageId()));
                    Assert.assertEquals("Unexpected Parent Page id.", DEFAULT_PARENT_PAGE_ID, s.getParentPageId());
                    Assert.assertEquals("Unexpected Pointer Key.", 0, COMPARATOR.compare(s.getPagePointer().getKey(), s.getItemAt(0)));

                    // Verify items are in order.
                    BTreeListPage.LeafPage splitPage = (BTreeListPage.LeafPage) s;
                    for (int i = 0; i < splitPage.getItemCount(); i++) {
                        val expectedItem = expectedItemIterator.next();
                        val item = splitPage.getItemAt(i);
                        Assert.assertEquals(0, COMPARATOR.compare(expectedItem, item));
                    }
                }

                Assert.assertFalse("Not all items were included in the splits.", expectedItemIterator.hasNext());
            } else {
                Assert.assertNull("Not expecting a split.", splitResult);
            }

            maxPageSize /= 2;
        }
    }

    /**
     * Tests the ability to split an Index Page.
     */
    @Test
    public void testIndexPageSplit() {

    }

    /**
     * Tests Empty {@link BTreeListPage.LeafPage}s and {@link BTreeListPage.IndexPage}s.
     */
    @Test
    public void testEmptyPage() {
        testEmptyPage(new BTreeListPage.LeafPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID));
        testEmptyPage(new BTreeListPage.IndexPage(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID));
    }

    private void testEmptyPage(BTreeListPage page) {
        Assert.assertEquals(0, page.getItemCount());
        Assert.assertEquals(0, page.getContentSize());
        Assert.assertEquals(BTreeListPage.HEADER_FOOTER_LENGTH, page.size());

        val page2 = BTreeListPage.parse(DEFAULT_PAGE_INFO, DEFAULT_PARENT_PAGE_ID, page.getContents());
        assertPageEquals(page, page2);
    }

    private List<BTreeListPage.UpdateItem> createUpdateBatch(int batchSize) {
        val updateBatch = new ArrayList<BTreeListPage.UpdateItem>();
        for (int i = 0; i < batchSize; i++) {
            updateBatch.add(new BTreeListPage.UpdateItem(newItem(), false));
        }

        return updateBatch;
    }

    private ArrayView newItem() {
        val item = new byte[Math.max(1, random.nextInt(MAX_ITEM_LENGTH))];
        random.nextBytes(item);
        return new ByteArraySegment(item);
    }

    private void check(BTreeListPage.IndexPage page, List<BTreeListPage.PagePointer> pagePointers, List<ArrayView> allItems) {
        Assert.assertEquals(pagePointers.size(), page.getItemCount());
        int nextPageInfoIndex = 0;
        for (val item : allItems) {
            if (nextPageInfoIndex < pagePointers.size() && COMPARATOR.compare(item, pagePointers.get(nextPageInfoIndex).getKey()) >= 0) {
                nextPageInfoIndex++;
            }

            val actualPointer = page.getChildPage(item, 0);
            if (nextPageInfoIndex == 0) {
                // Sought item is smaller than first key.
                Assert.assertNull(actualPointer);
            } else {
                // Sought item must have a result.
                val expectedPointer = pagePointers.get(nextPageInfoIndex - 1);
                Assert.assertEquals(expectedPointer.getPageId(), actualPointer.getPageId());
                Assert.assertEquals(0, COMPARATOR.compare(expectedPointer.getKey(), actualPointer.getKey()));
            }
        }
    }

    private void check(BTreeListPage.LeafPage page, TreeSet<ArrayView> expectedItems) {
        Assert.assertEquals(expectedItems.size(), page.getItemCount());
        int i = 0;
        for (val expected : expectedItems) {
            // First check using GeItemAt (since we know the position).
            val actual = page.getItemAt(i);
            Assert.assertEquals("Unexpected item at position " + i, 0, COMPARATOR.compare(expected, actual));

            // Then check using search.
            val searchResult = page.search(expected, 0);
            Assert.assertTrue("Expected an exact match.", searchResult.isExactMatch());
            Assert.assertEquals("Unexpected position from search().", i, searchResult.getPosition());
            i++;
        }
    }

    private void assertPageEquals(BTreeListPage p1, BTreeListPage p2) {
        Assert.assertSame(p1.getClass(), p2.getClass());
        Assert.assertEquals(p1.getItemCount(), p2.getItemCount());
        Assert.assertEquals(p1.getContentSize(), p2.getContentSize());
        Assert.assertEquals(p1.size(), p2.size());
        Assert.assertEquals(0, COMPARATOR.compare(p1.getContents(), p2.getContents()));
    }
}
