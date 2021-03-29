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

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SimpleDeque} class.
 */
public class SimpleDequeueTests {
    /**
     * Tests basic Dequeue operations (add, peek, poll, etc).
     */
    @Test
    public void testBasicDeque() {
        final int count = 12345;

        val s = new SimpleDeque<Integer>();
        Assert.assertTrue(s.isEmpty());
        Assert.assertEquals(0, s.size());
        Assert.assertNull(s.peekFirst());
        Assert.assertNull(s.pollFirst());
        Assert.assertTrue(s.pollFirst(10).isEmpty());

        // addLast.
        for (int i = 0; i < count; i++) {
            s.addLast(i);

            Assert.assertEquals(0, (int) s.peekFirst());
            Assert.assertFalse(s.isEmpty());
            Assert.assertEquals(i + 1, s.size());
        }

        // pollFirst and peekFirst.
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(i, (int) s.peekFirst());
            Assert.assertEquals(i, (int) s.pollFirst());
            Assert.assertEquals(count - i - 1, s.size());
            Assert.assertEquals(s.size() == 0, s.isEmpty());
        }

        for (int i = 0; i < count; i++) {
            s.addLast(i);
        }

        // Clear
        Assert.assertFalse(s.isEmpty());
        Assert.assertEquals(count, s.size());
        s.clear();

        Assert.assertTrue(s.isEmpty());
        Assert.assertEquals(0, s.size());
    }

    /**
     * Tests {@link SimpleDeque#pollFirst(int)}.
     */
    @Test
    public void testPollFirstMultiple() {
        final int count = 12345;

        val s = new SimpleDeque<Integer>();
        Assert.assertTrue(s.isEmpty());
        Assert.assertEquals(0, s.size());
        Assert.assertNull(s.peekFirst());
        Assert.assertNull(s.pollFirst());
        Assert.assertTrue(s.pollFirst(10).isEmpty());

        for (int i = 0; i < count; i++) {
            s.addLast(i);
        }

        // pollFirst and peekFirst.
        int batchSize = 0;
        int nextExpectedItem = 0;
        while (nextExpectedItem < count) {
            val expectedResultSize = Math.min(batchSize, count - nextExpectedItem);
            val expectedItems = IntStream.range(nextExpectedItem, nextExpectedItem + expectedResultSize).boxed()
                    .collect(Collectors.toList());

            val r = s.pollFirst(batchSize);
            checkPollFirstResult(expectedItems, r);

            nextExpectedItem += batchSize;
            batchSize++;
        }

        Assert.assertTrue(s.isEmpty());
        Assert.assertEquals(0, s.size());
    }

    /**
     * Tests a number or arbitrarily-applied operations.
     */
    @Test
    public void testArbitraryOperations() {
        final int count = 12345;
        int nextToAdd = 0;
        val rnd = new Random(0);
        val s = new SimpleDeque<Integer>();
        val a = new ArrayDeque<Integer>(); // Using this a source-of-truth.
        for (int i = 0; i < count; i++) {
            if (i % 10 == 0) {
                // PollFirst(many)
                final int pollSize = rnd.nextInt(10) + 1;
                val actual = s.pollFirst(pollSize);
                val expected = new ArrayList<Integer>();
                int pollRemaining = pollSize;
                while (pollRemaining > 0 && !a.isEmpty()) {
                    expected.add(a.pollFirst());
                    pollRemaining--;
                }

                checkPollFirstResult(expected, actual);
            }
            if (i % 3 == 0) {
                // PollFirst (one)
                val expected = a.pollFirst();
                val actual = s.pollFirst();
                Assert.assertEquals(expected, actual);
            }

            // Add
            s.addLast(nextToAdd);
            a.addLast(nextToAdd);
            nextToAdd++;
        }

        val finalActual = s.pollFirst(Integer.MAX_VALUE);
        val finalExpected = new ArrayList<>(a);
        checkPollFirstResult(finalExpected, finalActual);
    }

    private void checkPollFirstResult(List<Integer> expected, Queue<Integer> actual) {
        // Unsupported methods.
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> actual.offer(0));
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> actual.remove(0));
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> actual.removeAll(Collections.<Integer>emptyList()));
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> actual.retainAll(Collections.<Integer>emptyList()));
        AssertExtensions.assertThrows(UnsupportedOperationException.class, actual::clear);

        Assert.assertEquals(expected.size(), actual.size());
        val iteratorElements = new ArrayList<>(actual); // This invokes the iterator.
        Assert.assertEquals(actual.size(), iteratorElements.size());

        for (int i = 0; i < expected.size(); i++) {
            int e = expected.get(i);
            Assert.assertEquals("Iterator element mismatch.", e, (int) iteratorElements.get(i));
            Assert.assertEquals("Peek.", e, (int) actual.peek());
            Assert.assertEquals("Element.", e, (int) actual.element());
            if (i % 2 == 0) {
                Assert.assertEquals("Poll", e, (int) actual.poll());
            } else {
                Assert.assertEquals("Remove", e, (int) actual.remove());
            }

            Assert.assertEquals(expected.size() - i - 1, actual.size());
        }

        Assert.assertTrue(actual.isEmpty());
        Assert.assertNull(actual.peek());
        AssertExtensions.assertThrows(NoSuchElementException.class, actual::element);
        Assert.assertNull(actual.poll());
        AssertExtensions.assertThrows(NoSuchElementException.class, actual::remove);
    }
}
