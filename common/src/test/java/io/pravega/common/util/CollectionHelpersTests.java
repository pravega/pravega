/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the CollectionHelpers class.
 */
public class CollectionHelpersTests {
    /**
     * Tests the binarySearch() method on a List.
     */
    @Test
    public void testBinarySearchList() {
        int maxSize = 100;
        int skip = 3;
        ArrayList<Integer> list = new ArrayList<>();
        for (int size = 0; size < maxSize; size++) {
            int maxSearchElement = (list.size() + 1) * skip;
            for (AtomicInteger search = new AtomicInteger(-1); search.incrementAndGet() < maxSearchElement; ) {
                int expectedIndex = list.indexOf(search.get());
                int actualIndex = CollectionHelpers.binarySearch(list, i -> Integer.compare(search.get(), i));
                Assert.assertEquals("Unexpected result for search = " + search + " for list of size " + list.size(), expectedIndex, actualIndex);
            }
            // Add an element.
            list.add(maxSearchElement);
        }
    }

    /**
     * Tests the binarySearch() method on a IndexedMap.
     */
    @Test
    public void testBinarySearchSortedMap() {
        int maxSize = 100;
        int skip = 3;
        val m = new TestIndexedMap();
        val allValues = new HashMap<Integer, String>();
        val r = new Random(0);
        for (int size = 0; size < maxSize; size++) {
            // Generate search keys.
            int maxSearchElement = (m.getCount() + 1) * skip;
            val searchKeys = new ArrayList<Integer>();
            for (int i = 0; i < size; i += skip) {
                searchKeys.add(r.nextInt(size * 2));
            }

            val expectedValues = new HashMap<Integer, String>();
            searchKeys.stream()
                      .filter(allValues::containsKey)
                      .forEach(k -> expectedValues.put(k, allValues.get(k)));

            val actualValues = new HashMap<Integer, String>();
            val anythingFound = CollectionHelpers.binarySearch(m, searchKeys, actualValues);

            Assert.assertEquals("Unexpected return value for size " + size, expectedValues.size() > 0, anythingFound);
            AssertExtensions.assertMapEquals("Unexpected result contents for size " + size, expectedValues, actualValues);

            // Add new data.
            m.add(maxSearchElement, Integer.toString(maxSearchElement));
            allValues.put(maxSearchElement, Integer.toString(maxSearchElement));
        }
    }

    @Test
    public void testSearchLessThanEq() {
        List<TestElement> list = Lists.newArrayList(new TestElement(10L), new TestElement(30L), new TestElement(75L), 
                new TestElement(100L), new TestElement(152L), new TestElement(200L), new TestElement(400L), new TestElement(700L));

        BiFunction<TestElement, Long, Integer> func = (r, s) -> Long.compare(r.getElement(), s);
        int index = CollectionHelpers.searchLessThanEq(list, 0L, func);
        assertEquals(index, -1);
        index = CollectionHelpers.searchLessThanEq(list, 29L, func);
        assertEquals(index, 0);
        index = CollectionHelpers.searchLessThanEq(list, 101L, func);
        assertEquals(index, 3);
        index = CollectionHelpers.searchLessThanEq(list, Long.MAX_VALUE, func);
        assertEquals(index, 7);
    }

    /**
     * Tests the filterOut method.
     */
    @Test
    public void testFilterOut() {
        int size = 100;
        val collection = createCollection(0, size);

        val emptyRemoveResult = CollectionHelpers.filterOut(collection, Collections.emptySet());
        AssertExtensions.assertContainsSameElements("Unexpected result with empty toExclude.", collection, emptyRemoveResult);

        val noMatchResult = CollectionHelpers.filterOut(collection, Collections.singleton(size + 1));
        AssertExtensions.assertContainsSameElements("Unexpected result with no-match toExclude.", collection, noMatchResult);

        for (int i = 0; i < size; i++) {
            val toExclude = createCollection(0, i);
            val expectedResult = createCollection(i, size);
            val filterResult = CollectionHelpers.filterOut(collection, toExclude);
            AssertExtensions.assertContainsSameElements("Unexpected result from filterOut for i = " + i, expectedResult, filterResult);
        }
    }

    /**
     * Tests the joinSets() method.
     */
    @Test
    public void testJoinSets() {
        AssertExtensions.<Integer>assertContainsSameElements("Empty set.", Collections.emptySet(),
                CollectionHelpers.joinSets(Collections.emptySet(), Collections.emptySet()));
        AssertExtensions.assertContainsSameElements("Empty+non-empty.", Sets.newHashSet(1, 2, 3),
                CollectionHelpers.joinSets(Collections.emptySet(), Sets.newHashSet(1, 2, 3)));
        AssertExtensions.assertContainsSameElements("Non-empty+empty.", Sets.newHashSet(1, 2, 3),
                CollectionHelpers.joinSets(Sets.newHashSet(1, 2, 3), Collections.emptySet()));
        AssertExtensions.assertContainsSameElements("Non-empty+non-empty.", Sets.newHashSet(1, 2, 3),
                CollectionHelpers.joinSets(Sets.newHashSet(1, 3), Sets.newHashSet(2)));
        AssertExtensions.assertContainsSameElements("Non-empty+non-empty(duplicates).", Arrays.asList(1, 2, 2, 3),
                CollectionHelpers.joinSets(Sets.newHashSet(1, 2), Sets.newHashSet(2, 3)));
    }

    /**
     * Tests the joinCollections() method.
     */
    @Test
    public void testJoinCollections() {
        AssertExtensions.assertContainsSameElements("Empty set.", Collections.<Integer>emptySet(),
                CollectionHelpers.joinCollections(Collections.<Integer>emptySet(), i -> i, Collections.<Integer>emptySet(), i -> i));
        AssertExtensions.assertContainsSameElements("Empty+non-empty.", Arrays.asList(1, 2, 3),
                CollectionHelpers.joinCollections(Collections.<Integer>emptySet(), i -> i, Arrays.asList("1", "2", "3"), Integer::parseInt));
        AssertExtensions.assertContainsSameElements("Non-empty+empty.", Arrays.asList(1, 2, 3),
                CollectionHelpers.joinCollections(Arrays.asList("1", "2", "3"), Integer::parseInt, Collections.<Integer>emptySet(), i -> i));
        AssertExtensions.assertContainsSameElements("Non-empty+non-empty.", Arrays.asList(1, 2, 3),
                CollectionHelpers.joinCollections(Arrays.asList("1", "3"), Integer::parseInt, Arrays.asList("2"), Integer::parseInt));
        val c = CollectionHelpers.joinCollections(Arrays.asList("1", "2"), Integer::parseInt, Arrays.asList("2", "3"), Integer::parseInt);
        AssertExtensions.assertContainsSameElements("Non-empty+non-empty(duplicates).", Arrays.asList(1, 2, 2, 3), c);

        // Now test the iterator.
        val copy = new ArrayList<Integer>(c);
        AssertExtensions.assertContainsSameElements("Non-empty+non-empty(duplicates, copy).", c, copy);
    }

    private Collection<Integer> createCollection(int from, int upTo) {
        Collection<Integer> collection = new HashSet<>(upTo - from);
        for (int i = from; i < upTo; i++) {
            collection.add(i);
        }

        return collection;
    }

    private static class TestIndexedMap implements IndexedMap<Integer, String> {
        private final ArrayList<Map.Entry<Integer, String>> entries = new ArrayList<>();

        void add(int key, String value) {
            this.entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }

        @Override
        public int getCount() {
            return this.entries.size();
        }

        @Override
        public Integer getKey(int position) {
            return this.entries.get(position).getKey();
        }

        @Override
        public String getValue(int position) {
            return this.entries.get(position).getValue();
        }
    }
    
    @Data
    private static class TestElement {
        private final long element;
    }
}
