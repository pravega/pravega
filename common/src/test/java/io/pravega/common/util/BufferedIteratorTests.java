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
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.stream.LongStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BufferedIterator} class.
 */
public class BufferedIteratorTests {
    private static final int BATCH_SIZE = 16;
    private static final long MAX_INDEX = 123;

    /**
     * Tests the case when invalid args are passed to the constructor.
     */
    @Test
    public void testInvalidArgs() {
        // First Index > Last Index.
        val i1 = newIterator(10, 9, BATCH_SIZE);
        Assert.assertFalse("Expected empty iterator.", i1.hasNext());
        AssertExtensions.assertThrows(
                "empty iterator should have thrown.",
                i1::next,
                ex -> ex instanceof NoSuchElementException);

        // Returning empty iterators (against the contract).
        val i2 = new BufferedIterator<>((from, to) -> Collections.emptyIterator(), 0, MAX_INDEX, BATCH_SIZE);
        Assert.assertTrue("Not expected empty iterator.", i2.hasNext());
        AssertExtensions.assertThrows(
                "Expected exception when getItemRange returned empty iterator.",
                i2::next,
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testNoBatching() {
        testWithBatchSize(Integer.MAX_VALUE);
    }

    @Test
    public void testSingularBatch() {
        testWithBatchSize(1);
    }

    @Test
    public void testMultiBatch() {
        testWithBatchSize(BATCH_SIZE);
    }

    private void testWithBatchSize(int batchSize) {
        for (int firstIndex = 0; firstIndex <= MAX_INDEX; firstIndex++) {
            for (int lastIndex = firstIndex; lastIndex <= MAX_INDEX; lastIndex++) {
                val i = newIterator(firstIndex, lastIndex, batchSize);

                String context = String.format("FirstIndex=%d, LastIndex=%d", firstIndex, lastIndex);
                for (long expected = firstIndex; expected <= lastIndex; expected++) {
                    Assert.assertTrue("Expecting iterator to have next value " + context, i.hasNext());
                    long actual = i.next();
                    Assert.assertEquals("Unexpected value " + context, expected, actual);
                }

                Assert.assertFalse("Expected iterator to have completed " + context, i.hasNext());
            }
        }
    }

    private BufferedIterator<Long> newIterator(long firstIndex, long lastIndex, int batchSize) {
        return new BufferedIterator<>((from, to) -> LongStream.rangeClosed(from, to).iterator(), firstIndex, lastIndex, batchSize);
    }


}
