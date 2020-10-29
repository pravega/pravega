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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * Unit tests for the {@link BTreeSetPage} class.
 */
public class BTreeSetPageTests {
    private static final int DEFAULT_PAGE_ID = 2;
    private static final int DEFAULT_PARENT_PAGE_ID = 1;
    private static final PagePointer DEFAULT_PAGE_INFO = new PagePointer(null, DEFAULT_PAGE_ID, DEFAULT_PARENT_PAGE_ID);
    private static final int MAX_ITEM_LENGTH = 4096;
    private static final Comparator<ArrayView> COMPARATOR = BTreeSet.COMPARATOR;
    private final Random random = new Random(0);

    /**
     * Tests {@link BTreeSetPage.LeafPage#update(List)} with insertions.
     */
    @Test
    public void testLeafPageInsert() {
        val count = 3000;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);
        Assert.assertFalse(page.isModified());
        int updateCount = 0;
        int batchSize = 1;
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        val insertedKeys = new ArrayList<ArrayView>();
        while (updateCount < count) {
            val updateBatch = createUpdateBatch(batchSize);
            updateBatch.forEach(i -> expectedItems.add(i.getItem()));

            // Add some random key that has already been inserted.
            if (insertedKeys.size() > 0) {
                updateBatch.add(new UpdateItem(insertedKeys.get(random.nextInt(insertedKeys.size())), false));
            }
            updateBatch.forEach(i -> insertedKeys.add(i.getItem()));

            // Apply the update and verify it.
            updateBatch.sort(UpdateItem::compareTo);
            page.update(updateBatch);
            Assert.assertTrue(page.isModified());
            check(page, expectedItems);

            updateCount += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeSetPage.LeafPage#update(List)} with deletions.
     */
    @Test
    public void testLeafPageDelete() {
        val count = 2000;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);

        // Bulk-update the page.
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        val updateBatch = createUpdateBatch(count);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(UpdateItem::compareTo);
        page.update(updateBatch);
        Assert.assertTrue(page.isModified());
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
                    .stream().map(item -> new UpdateItem(item, true)).collect(Collectors.toList());
            removeBatch.forEach(u -> expectedItems.remove(u.getItem()));
            removeBatch.sort(UpdateItem::compareTo);

            // Apply it and verify the page.
            page.update(removeBatch);
            Assert.assertTrue(page.isModified());
            check(page, expectedItems);

            index += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeSetPage.LeafPage#update(List)} with both insertions and deletions.
     */
    @Test
    public void testLeafPageUpdateDelete() {
        val iterationCount = 200;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);
        int iterationId = 0;
        int batchSize = 1;
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);

        while (iterationId < iterationCount) {
            int removalCount = random.nextInt(batchSize);
            val shuffledItems = new ArrayList<ArrayView>(expectedItems);
            Collections.shuffle(shuffledItems, random);

            // Add some updates
            val updateBatch = createUpdateBatch(batchSize - removalCount);

            // Remove some existing keys.
            int actualRemovalCount = Math.min(removalCount, shuffledItems.size());
            for (int i = 0; i < actualRemovalCount; i++) {
                updateBatch.add(new UpdateItem(shuffledItems.get(i), true));
            }

            // Remove some extra (not present) keys.
            for (int i = actualRemovalCount; i < removalCount; i++) {
                updateBatch.add(new UpdateItem(newItem(), true));
            }

            updateBatch.sort(UpdateItem::compareTo);
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
     * Tests {@link BTreeSetPage.IndexPage#addChildren}.
     */
    @Test
    public void testIndexPageAddChildren() {
        val childPageCount = 1000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new PagePointer(allItems.get(i), i, DEFAULT_PARENT_PAGE_ID))
                .collect(Collectors.toList());

        val page = new BTreeSetPage.IndexPage(DEFAULT_PAGE_INFO);
        Assert.assertFalse(page.isModified());
        Assert.assertNull(page.getChildPage(newItem(), 0));

        int updateCount = 0;
        int batchSize = 1;
        val addedPointers = new ArrayList<PagePointer>();
        while (updateCount < allPointers.size()) {
            // Generate an update batch.
            batchSize = Math.min(batchSize, allPointers.size() - updateCount);
            val p = allPointers.subList(updateCount, updateCount + batchSize);

            // Add it.
            page.addChildren(p);
            Assert.assertTrue(page.isModified());
            addedPointers.addAll(p);

            // And verify it.
            check(page, addedPointers, allItems);

            updateCount += batchSize;
            batchSize++;
        }
    }

    /**
     * Tests {@link BTreeSetPage.IndexPage#removeChildren}.
     */
    @Test
    public void testIndexPageRemoveChildren() {
        val childPageCount = 1000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new PagePointer(allItems.get(i), i, DEFAULT_PARENT_PAGE_ID))
                .collect(Collectors.toList());

        // Create a new page and add all items to it.
        val page = new BTreeSetPage.IndexPage(DEFAULT_PAGE_INFO);
        page.addChildren(allPointers);
        Assert.assertTrue(page.isModified());
        check(page, allPointers, allItems);

        List<PagePointer> shuffledPointers = new ArrayList<>(allPointers);
        Collections.shuffle(shuffledPointers);
        int batchSize = 1;
        while (!allPointers.isEmpty()) {
            // Generate a removal batch.
            batchSize = Math.min(batchSize, shuffledPointers.size());
            val batch = shuffledPointers.subList(0, batchSize);
            batch.sort((p1, p2) -> COMPARATOR.compare(p1.getKey(), p2.getKey()));

            // Apply it
            page.removeChildren(batch);
            Assert.assertTrue(page.isModified());
            shuffledPointers = shuffledPointers.subList(batchSize, shuffledPointers.size());

            // And verify it.
            allPointers.removeAll(batch);
            check(page, allPointers, allItems);

            batchSize++;
        }
    }

    /**
     * Tests the ability to split a Non-Root Leaf Page.
     */
    @Test
    public void testLeafPageSplit() {
        val itemCount = 5000;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);

        // Bulk-update the page.
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        val updateBatch = createUpdateBatch(itemCount);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(UpdateItem::compareTo);
        page.update(updateBatch);

        val nextPageId = new AtomicLong(1000);
        int maxPageSize = page.size();
        while (maxPageSize > MAX_ITEM_LENGTH) {
            val splitResult = page.split(maxPageSize, nextPageId::getAndIncrement);
            boolean requiresSplit = maxPageSize < page.size();
            if (requiresSplit) {
                verifyLeafPageSplit(splitResult, expectedItems, maxPageSize, Collections.singletonList(page.getPagePointer().getParentPageId()));
            } else {
                Assert.assertNull("Not expecting a split.", splitResult);
            }

            maxPageSize /= 2;
        }
    }

    /**
     * Tests the ability to split a Root Leaf Page recursively.
     */
    @Test
    public void testLeafPageSplitRecursive() {
        val itemCount = 5000;
        val page = new BTreeSetPage.LeafPage(PagePointer.root());

        // Bulk-update the page.
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        val updateBatch = createUpdateBatch(itemCount);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(UpdateItem::compareTo);
        page.update(updateBatch);

        val nextPageId = new AtomicLong(1000);
        int maxPageSize = page.size() / 2;
        List<BTreeSetPage> currentPages = new ArrayList<>();
        currentPages.add(page);
        while (maxPageSize > MAX_ITEM_LENGTH) {
            val splitResult = new ArrayList<BTreeSetPage>();
            for (val s : currentPages) {
                val sr = s.split(maxPageSize, nextPageId::getAndIncrement);
                if (sr == null) {
                    splitResult.add(s);
                } else {
                    splitResult.addAll(sr);
                }
            }

            // Generate the expected set of parent ids, whether we are splitting the root or a non-root node.
            val expectedParentIds = currentPages.size() == 1
                    ? Collections.singletonList(page.getPagePointer().getPageId())
                    : currentPages.stream().map(p -> p.getPagePointer().getParentPageId()).distinct().collect(Collectors.toList());
            verifyLeafPageSplit(splitResult, expectedItems, maxPageSize, expectedParentIds);
            currentPages = splitResult;
            maxPageSize /= 2;
        }
    }

    private void verifyLeafPageSplit(List<BTreeSetPage> splitResult, TreeSet<ArrayView> expectedItems, int maxPageSize, List<Long> parentPageIds) {
        // Verify counts match.
        val splitItemCount = splitResult.stream().mapToInt(BTreeSetPage::getItemCount).sum();
        Assert.assertEquals(expectedItems.size(), splitItemCount);

        // We will iterate through all split page's items in order and compare them against our expected order.
        val expectedItemIterator = expectedItems.iterator();
        val pageIds = new HashSet<Long>();
        int parentPageIndex = 0;
        for (val s : splitResult) {
            AssertExtensions.assertGreaterThan("Not expecting any empty split pages.", 0, s.size());
            AssertExtensions.assertLessThanOrEqual("Split page too large.", maxPageSize, s.size());
            Assert.assertTrue(s.isModified());

            // Verify page pointers are correct.
            Assert.assertTrue("Duplicate Page Id.", pageIds.add(s.getPagePointer().getPageId()));
            if (s.getPagePointer().getParentPageId() != parentPageIds.get(parentPageIndex)) {
                // We may have moved on to the next parent page.
                parentPageIndex++;
                Assert.assertEquals("Unexpected Parent Page id.", (long) parentPageIds.get(parentPageIndex), s.getPagePointer().getParentPageId());
            }

            Assert.assertEquals("Unexpected Pointer Key.", 0, COMPARATOR.compare(s.getPagePointer().getKey(), s.getItemAt(0)));

            // Verify items are in order.
            BTreeSetPage.LeafPage splitPage = (BTreeSetPage.LeafPage) s;
            for (int i = 0; i < splitPage.getItemCount(); i++) {
                val expectedItem = expectedItemIterator.next();
                val item = splitPage.getItemAt(i);
                Assert.assertEquals(0, COMPARATOR.compare(expectedItem, item));
            }

            // Verify that the split page is deserializable.
            assertPageEquals(s, BTreeSetPage.parse(s.getPagePointer(), s.getData()));
        }

        Assert.assertFalse("Not all items were included in the splits.", expectedItemIterator.hasNext());
    }

    /**
     * Tests the ability to split a Non-Root Index Page.
     */
    @Test
    public void testIndexPageSplit() {
        val childPageCount = 5000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new PagePointer(allItems.get(i), i, DEFAULT_PARENT_PAGE_ID))
                .collect(Collectors.toList());

        // Create a new page and add all items to it.
        val page = new BTreeSetPage.IndexPage(DEFAULT_PAGE_INFO);
        page.addChildren(allPointers);

        // Check single-level splits.
        val nextPageId = new AtomicLong(1000);
        int maxPageSize = page.size();
        while (maxPageSize > MAX_ITEM_LENGTH) {
            val splitResult = page.split(maxPageSize, nextPageId::getAndIncrement);
            boolean requiresSplit = maxPageSize < page.size();
            if (requiresSplit) {
                verifyIndexPageSplit(splitResult, allPointers, maxPageSize, Collections.singletonList(page.getPagePointer().getParentPageId()));
            } else {
                Assert.assertNull("Not expecting a split.", splitResult);
            }

            maxPageSize /= 2;
        }
    }

    /**
     * Tests the ability to split a Root Index Page recursively.
     */
    @Test
    public void testIndexPageSplitRecursive() {
        val childPageCount = 5000;
        val itemsPerPage = 2;
        val itemCount = childPageCount * itemsPerPage;
        val allItems = IntStream.range(0, itemCount).mapToObj(i -> newItem()).sorted(COMPARATOR).collect(Collectors.toList());
        val allPointers = IntStream.range(0, itemCount).filter(i -> i % itemsPerPage == 0)
                .mapToObj(i -> new PagePointer(allItems.get(i), i, DEFAULT_PARENT_PAGE_ID))
                .collect(Collectors.toList());

        // Create a new page and add all items to it.
        val page = new BTreeSetPage.IndexPage(PagePointer.root());
        page.addChildren(allPointers);

        // Check single-level splits.
        val nextPageId = new AtomicLong(1000);
        int maxPageSize = page.size() / 2;
        List<BTreeSetPage> currentPages = new ArrayList<>();
        currentPages.add(page);
        while (maxPageSize > MAX_ITEM_LENGTH) {
            val splitResult = new ArrayList<BTreeSetPage>();
            for (val s : currentPages) {
                val sr = s.split(maxPageSize, nextPageId::getAndIncrement);
                if (sr == null) {
                    splitResult.add(s);
                } else {
                    splitResult.addAll(sr);
                }
            }

            // Generate the expected set of parent ids, whether we are splitting the root or a non-root node.
            val expectedParentIds = currentPages.size() == 1
                    ? Collections.singletonList(page.getPagePointer().getPageId())
                    : currentPages.stream().map(p -> p.getPagePointer().getParentPageId()).distinct().collect(Collectors.toList());
            verifyIndexPageSplit(splitResult, allPointers, maxPageSize, expectedParentIds);
            currentPages = splitResult;
            maxPageSize /= 2;
        }
    }

    private void verifyIndexPageSplit(List<BTreeSetPage> splitResult, List<PagePointer> allPointers, int maxPageSize, List<Long> parentPageIds) {
        // Verify counts match.
        val splitItemCount = splitResult.stream().mapToInt(BTreeSetPage::getItemCount).sum();
        Assert.assertEquals(allPointers.size(), splitItemCount);

        // We will iterate through all split page's items in order and compare them against our expected order.
        val expectedPointerIterator = allPointers.iterator();
        val pageIds = new HashSet<Long>();
        int parentPageIndex = 0;
        for (val s : splitResult) {
            AssertExtensions.assertGreaterThan("Not expecting any empty split pages.", 0, s.size());
            AssertExtensions.assertLessThanOrEqual("Split page too large.", maxPageSize, s.size());
            Assert.assertTrue(s.isModified());

            // Verify page pointers are correct.
            Assert.assertTrue("Duplicate Page Id.", pageIds.add(s.getPagePointer().getPageId()));
            if (s.getPagePointer().getParentPageId() != parentPageIds.get(parentPageIndex)) {
                // We may have moved on to the next parent page.
                parentPageIndex++;
                Assert.assertEquals("Unexpected Parent Page id.", (long) parentPageIds.get(parentPageIndex), s.getPagePointer().getParentPageId());
            }

            // Verify items are in order.
            BTreeSetPage.IndexPage splitPage = (BTreeSetPage.IndexPage) s;
            for (int i = 0; i < splitPage.getItemCount(); i++) {
                val expectedItem = expectedPointerIterator.next();
                val item = splitPage.getChildPage(expectedItem.getKey(), 0);
                if (i == 0) {
                    // First entries are special. Their contents must equal MIN_KEY, while the corresponding
                    // key from the source page must be the page pointer
                    Assert.assertEquals("Unexpected Pointer Key.", 0,
                            COMPARATOR.compare(s.getPagePointer().getKey(), expectedItem.getKey()));
                    Assert.assertEquals("First item must be COMPARATOR.MIN_KEY.", 0,
                            COMPARATOR.compare(new ByteArraySegment(BufferViewComparator.getMinValue()), item.getKey()));
                    Assert.assertEquals("Unexpected PageId for first item.", 0,
                            expectedItem.getPageId(), item.getPageId());
                } else {
                    assertPointerEquals(expectedItem, item);
                }
            }

            // Verify that the split page is deserializable.
            assertPageEquals(s, BTreeSetPage.parse(s.getPagePointer(), s.getData()));
        }

        Assert.assertFalse("Not all items were included in the splits.", expectedPointerIterator.hasNext());
    }

    /**
     * Tests various cases with bad serialization passed to {@link BTreeSetPage#parse}.
     */
    @Test
    public void testBadSerialization() {
        val count = 10;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);
        val updateBatch = createUpdateBatch(count);
        updateBatch.sort(UpdateItem::compareTo);
        page.update(updateBatch);

        // Unsupported version.
        val badVersion = page.getData().getCopy();
        badVersion[0] = 123;
        AssertExtensions.assertThrows(
                "Unsupported version was allowed.",
                () -> BTreeSetPage.parse(page.getPagePointer(), new ByteArraySegment(badVersion)),
                ex -> ex instanceof IllegalDataFormatException);

        // Page Id mismatch.
        AssertExtensions.assertThrows(
                "Invalid Page Id was allowed.",
                () -> BTreeSetPage.parse(new PagePointer(null, 1234, DEFAULT_PARENT_PAGE_ID), page.getData()),
                ex -> ex instanceof IllegalDataFormatException);

        // Bad size
        val badSize = page.getData().getCopy();
        AssertExtensions.assertThrows(
                "Invalid Size was allowed.",
                () -> BTreeSetPage.parse(page.getPagePointer(), new ByteArraySegment(badSize, 0, badSize.length - 1)),
                ex -> ex instanceof IllegalDataFormatException);
    }

    /**
     * Tests the {@link BTreeSetPage#parse} with {@link BTreeSetPage.LeafPage} serializations.
     */
    @Test
    public void testLeafPageParse() {
        val count = 1000;
        val page = new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO);
        val expectedItems = new TreeSet<ArrayView>(COMPARATOR);
        val updateBatch = createUpdateBatch(count);
        updateBatch.forEach(i -> expectedItems.add(i.getItem()));
        updateBatch.sort(UpdateItem::compareTo);
        page.update(updateBatch);

        // In addition, we also want to verify that we can process a larger buffer and adjust it based on the encoded size.
        byte[] newBuffer = new byte[page.getData().getLength() * 2];
        page.getData().copyTo(newBuffer, 0, page.getData().getLength());
        page.getData().copyTo(newBuffer, page.getData().getLength(), page.getData().getLength());

        val page2 = (BTreeSetPage.LeafPage) BTreeSetPage.parse(page.getPagePointer(), new ByteArraySegment(newBuffer));
        Assert.assertSame(page.getPagePointer(), page2.getPagePointer());
        check(page2, expectedItems);
    }

    /**
     * Tests the {@link BTreeSetPage#parse} with {@link BTreeSetPage.IndexPage} serializations.
     */
    @Test
    public void testIndexPageParse() {
        val itemCount = 1000;
        val allPointers = IntStream.range(0, itemCount)
                .mapToObj(i -> newItem())
                .sorted(COMPARATOR)
                .map(key -> new PagePointer(key, random.nextLong(), DEFAULT_PARENT_PAGE_ID))
                .collect(Collectors.toList());

        // Create a new page and add all items to it.
        val page = new BTreeSetPage.IndexPage(DEFAULT_PAGE_INFO);
        page.addChildren(allPointers);

        val page2 = (BTreeSetPage.IndexPage) BTreeSetPage.parse(page.getPagePointer(), page.getData());
        Assert.assertSame(page.getPagePointer(), page2.getPagePointer());
        check(page2, allPointers, allPointers.stream().map(PagePointer::getKey).collect(Collectors.toList()));
    }

    /**
     * Tests Empty {@link BTreeSetPage.LeafPage}s and {@link BTreeSetPage.IndexPage}s.
     */
    @Test
    public void testEmptyPage() {
        testEmptyPage(new BTreeSetPage.LeafPage(DEFAULT_PAGE_INFO));
        testEmptyPage(new BTreeSetPage.IndexPage(DEFAULT_PAGE_INFO));
        testEmptyPage(BTreeSetPage.emptyLeafRoot());
        testEmptyPage(BTreeSetPage.emptyIndexRoot());
    }

    private void testEmptyPage(BTreeSetPage page) {
        Assert.assertEquals(0, page.getItemCount());
        Assert.assertEquals(0, page.getContentSize());
        Assert.assertEquals(BTreeSetPage.HEADER_FOOTER_LENGTH, page.size());

        val page2 = BTreeSetPage.parse(page.getPagePointer(), page.getData());
        assertPageEquals(page, page2);
    }

    private List<UpdateItem> createUpdateBatch(int batchSize) {
        val updateBatch = new ArrayList<UpdateItem>();
        for (int i = 0; i < batchSize; i++) {
            updateBatch.add(new UpdateItem(newItem(), false));
        }

        return updateBatch;
    }

    private ArrayView newItem() {
        val item = new byte[Math.max(1, random.nextInt(MAX_ITEM_LENGTH))];
        random.nextBytes(item);
        return new ByteArraySegment(item);
    }

    private void check(BTreeSetPage.IndexPage page, List<PagePointer> pagePointers, List<ArrayView> allItems) {
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
                assertPointerEquals(expectedPointer, actualPointer);
            }
        }
    }

    private void check(BTreeSetPage.LeafPage page, TreeSet<ArrayView> expectedItems) {
        Assert.assertEquals(expectedItems.size(), page.getItemCount());
        List<ArrayView> allItems;
        if (expectedItems.isEmpty()) {
            AssertExtensions.assertThrows(
                    "getItems worked when empty.",
                    () -> page.getItems(0, page.getItemCount()),
                    ex -> ex instanceof IllegalArgumentException);
            allItems = Collections.emptyList();
        } else {
            allItems = page.getItems(0, page.getItemCount() - 1);
            Assert.assertEquals(expectedItems.size(), allItems.size());
        }

        int i = 0;
        for (val expected : expectedItems) {
            // First check using GeItemAt (since we know the position).
            val actual = page.getItemAt(i);
            Assert.assertEquals("Unexpected item at position " + i, 0, COMPARATOR.compare(expected, actual));

            // Then validate the item from allItems.
            Assert.assertEquals("Unexpected item (getAllItems) at position " + i, 0, COMPARATOR.compare(expected, allItems.get(i)));

            // Then check using search.
            val searchResult = page.search(expected, 0);
            Assert.assertTrue("Expected an exact match.", searchResult.isExactMatch());
            Assert.assertEquals("Unexpected position from search().", i, searchResult.getPosition());
            i++;
        }

        if (allItems.size() > 0) {
            // Verify getItems with range searches.
            int skip = Math.max(1, allItems.size() / 10); // This is called a lot; skip over some indices to save time.
            for (int a = skip; a < allItems.size() / 2; a += skip) {
                val subRange = page.getItems(a, allItems.size() - a - 1);
                Assert.assertEquals(allItems.size() - 2 * a, subRange.size());
                for (int j = 0; j < subRange.size(); j++) {
                    Assert.assertEquals("Unexpected item (sub-range) at position " + (a + j),
                            0, COMPARATOR.compare(allItems.get(a + j), subRange.get(j)));
                }
            }
        }
    }

    private void assertPointerEquals(PagePointer p1, PagePointer p2) {
        Assert.assertEquals(p1.getPageId(), p2.getPageId());
        Assert.assertEquals(0, COMPARATOR.compare(p1.getKey(), p2.getKey()));
    }

    private void assertPageEquals(BTreeSetPage p1, BTreeSetPage p2) {
        Assert.assertSame(p1.getClass(), p2.getClass());
        Assert.assertEquals(p1.getItemCount(), p2.getItemCount());
        Assert.assertEquals(p1.getContentSize(), p2.getContentSize());
        Assert.assertEquals(p1.size(), p2.size());
        Assert.assertEquals(0, COMPARATOR.compare(p1.getData(), p2.getData()));
    }
}

