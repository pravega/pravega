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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link PriorityBlockingDrainingQueue} class.
 */
public class PriorityBlockingDrainingQueueTests {
    private static final byte MAX_PRIORITY = 10;
    private static final int TIMEOUT_MILLIS = 10 * 1000;

    /**
     * Tests the ability to reject items with invalid priorities.
     */
    @Test
    public void testInvalidPriorities() {
        AssertExtensions.assertThrows(
                "negative max priority value.",
                () -> new PriorityBlockingDrainingQueue<TestItem>((byte) -1),
                ex -> ex instanceof IllegalArgumentException);

        @Cleanup
        val q = new PriorityBlockingDrainingQueue<TestItem>(MAX_PRIORITY);
        AssertExtensions.assertThrows("priority value exceeds max.",
                () -> q.add(new TestItem(1, (byte) 11)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("negative priority value.",
                () -> q.add(new TestItem(1, (byte) -1)),
                ex -> ex instanceof IllegalArgumentException);

        Assert.assertEquals(0, q.size());
        Assert.assertEquals(0, q.close().size());
        Assert.assertTrue(q.isClosed());
    }

    /**
     * Tests the ability to function with a single priority. In this case, {@link PriorityBlockingDrainingQueue} should
     * behave identically to {@link BlockingDrainingQueue}.
     */
    @Test
    public void testSinglePriority() throws Exception {
        final byte priority = 5;
        @Cleanup
        val q = new PriorityBlockingDrainingQueue<TestItem>(MAX_PRIORITY);

        // Pending Take.
        val take1 = q.take(5);
        Assert.assertFalse("Not expecting a take() to be complete yet.", take1.isDone());
        val item1 = new TestItem(1, priority);
        q.add(item1);
        val take1Result = take1.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of elements in the take() result.", 1, take1Result.size());
        Assert.assertSame("Unexpected item returned.", item1, take1Result.peek());
        Assert.assertEquals("Expected empty queue.", 0, q.size());

        // Take with existing items.
        val item2 = new TestItem(2, priority);
        val item3 = new TestItem(3, priority);
        q.add(item1);
        q.add(item2);
        q.add(item3);
        val take2 = q.take(2);
        Assert.assertTrue("Expected take(with items) to complete immediately.", take2.isDone());
        val take2Result = take2.join();
        Assert.assertEquals("Unexpected number of elements in the take() result.", 2, take2Result.size());
        Assert.assertSame("Unexpected first item returned.", item1, take2Result.poll());
        Assert.assertSame("Unexpected second item returned.", item2, take2Result.poll());
        Assert.assertEquals("Expected non-empty queue.", 1, q.size());
        val poll2Result = q.poll(2);
        Assert.assertEquals("Unexpected number of elements in the take() result.", 1, poll2Result.size());
        Assert.assertSame("Unexpected third item returned.", item3, poll2Result.poll());
        Assert.assertEquals("Expected empty queue.", 0, q.size());

        Assert.assertEquals("Not expecting any more items.", 0, q.poll(10).size());
    }

    /**
     * Tests the ability to function with multiple priorities.
     */
    @Test
    public void testMultiplePriorities() {
        val priorities = Arrays.asList((byte) 5, (byte) 3, (byte) 7);
        val itemsPerPriority = 3;
        @Cleanup
        val q = new PriorityBlockingDrainingQueue<TestItem>(MAX_PRIORITY);
        val expectedItems = new ConcurrentSkipListSet<>(new TestItemComparator());

        // Add a number of items with arbitrary priorities and validate peek() and size().
        for (int i = 0; i < itemsPerPriority; i++) {
            for (byte p : priorities) {
                val item = new TestItem(expectedItems.size(), p);
                q.add(item);
                expectedItems.add(item);
                Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
                Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());
            }
        }

        // Test take().
        val take1 = q.take(itemsPerPriority + 1);
        Assert.assertTrue(take1.isDone());
        val take1Result = take1.join();
        val expectedTake1Result = getFirstItems(expectedItems, itemsPerPriority);
        assertSame(expectedTake1Result, take1Result);
        Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
        Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());

        // Test poll().
        val poll1Result = q.poll(itemsPerPriority + 1);
        val expectedPoll1Result = getFirstItems(expectedItems, itemsPerPriority);
        assertSame(expectedPoll1Result, poll1Result);
        Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
        Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());

        // Test take() and poll() on whatever is left.
        val take2 = q.take(1);
        Assert.assertTrue(take2.isDone());
        val take2Result = take2.join();
        val expectedTake2Result = getFirstItems(expectedItems, 1);
        assertSame(expectedTake2Result, take2Result);
        Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
        Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());

        val poll2Result = q.poll(1);
        val expectedPoll2Result = getFirstItems(expectedItems, 1);
        assertSame(expectedPoll2Result, poll2Result);
        Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
        Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());

        // Add another priority and verify it's properly recorded.
        val finalPriority = (byte) 0;
        val finalItem = new TestItem(0, finalPriority);
        q.add(finalItem);
        expectedItems.add(finalItem);
        Assert.assertEquals("Unexpected size.", expectedItems.size(), q.size());
        Assert.assertSame("Unexpected peek.", expectedItems.first(), q.peek());

        val finalPoll = new ArrayDeque<>(q.poll(100)); // Make a copy since we need to modify it.
        Assert.assertEquals(1, finalPoll.size());
        finalPoll.addAll(q.poll(100));
        val expectedFinalPoll = getFirstItems(expectedItems, 2);
        assertSame(expectedFinalPoll, finalPoll);
        Assert.assertEquals("Unexpected final size.", 0, q.size());
        Assert.assertNull("Unexpected final peek.", q.peek());
    }

    /**
     * Tests the {@link PriorityBlockingDrainingQueue#close()} method when there are items with multiple priorities in
     * the queue.
     */
    @Test
    public void testClose() {
        @Cleanup
        val q = new PriorityBlockingDrainingQueue<TestItem>(MAX_PRIORITY);
        Assert.assertFalse(q.isClosed());

        // Add 3 items with different priority levels.
        val item1 = new TestItem(1, (byte) 5);
        val item2 = new TestItem(2, (byte) 3);
        val item3 = new TestItem(3, (byte) 7);
        q.add(item1);
        q.add(item2);
        q.add(item3);

        // Close and verify that the result contains all the items added, but in the correct priority order.
        val closeResult = q.close();
        Assert.assertEquals("Unexpected result size from close.", 3, closeResult.size());
        Assert.assertSame("Unexpected result item from close", item2, closeResult.poll());
        Assert.assertSame("Unexpected result item from close", item1, closeResult.poll());
        Assert.assertSame("Unexpected result item from close", item3, closeResult.poll());
        Assert.assertTrue(q.isClosed());
    }

    private Queue<TestItem> getFirstItems(ConcurrentSkipListSet<TestItem> set, int count) {
        val result = new ArrayDeque<TestItem>();
        for (int i = 0; i < count; i++) {
            result.addLast(set.pollFirst());
        }
        return result;
    }

    private void assertSame(Queue<TestItem> expected, Queue<TestItem> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        while (!expected.isEmpty()) {
            val e = expected.poll();
            val a = actual.poll();
            Assert.assertSame(e, a);
        }
    }

    @Data
    private static class TestItem implements PriorityBlockingDrainingQueue.Item {
        private final int value;
        private final byte priorityValue;
    }

    private static class TestItemComparator implements Comparator<TestItem> {

        @Override
        public int compare(TestItem t1, TestItem t2) {
            int i = Byte.compare(t1.getPriorityValue(), t2.getPriorityValue());
            if (i == 0) {
                return Integer.compare(t1.getValue(), t2.getValue());
            }
            return i;
        }
    }
}
