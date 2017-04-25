/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CollectionHelpers class.
 */
public class CollectionHelpersTests {
    /**
     * Tests the binary search method.
     */
    @Test
    public void testBinarySearch() {
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

    private Collection<Integer> createCollection(int from, int upTo) {
        Collection<Integer> collection = new HashSet<>(upTo - from);
        for (int i = from; i < upTo; i++) {
            collection.add(i);
        }

        return collection;
    }
}
