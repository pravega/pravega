/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the Collections class.
 */
public class CollectionsTests {
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
                int actualIndex = Collections.binarySearch(list, i -> Integer.compare(search.get(), i));
                Assert.assertEquals("Unexpected result for search = " + search + " for list of size " + list.size(), expectedIndex, actualIndex);
            }
            // Add an element.
            list.add(maxSearchElement);
        }
    }
}
