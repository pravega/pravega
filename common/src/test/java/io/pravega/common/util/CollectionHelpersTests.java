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
package io.pravega.common.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    public void testSearchGLB() {
        List<TestElement> list = Lists.newArrayList(new TestElement(10L), new TestElement(30L), new TestElement(75L),
                new TestElement(100L), new TestElement(152L), new TestElement(200L), new TestElement(400L), new TestElement(700L));

        int index = CollectionHelpers.findGreatestLowerBound(list, x -> Long.compare(0L, x.getElement()));
        assertEquals(index, -1);
        index = CollectionHelpers.findGreatestLowerBound(list, x -> Long.compare(29, x.getElement()));
        assertEquals(index, 0);
        index = CollectionHelpers.findGreatestLowerBound(list, x -> Long.compare(100L, x.getElement()));
        assertEquals(index, 3);
        index = CollectionHelpers.findGreatestLowerBound(list, x -> Long.compare(Long.MAX_VALUE, x.getElement()));
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

    @Data
    private static class TestElement {
        private final long element;
    }
}
