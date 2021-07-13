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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.IteratorItem;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableSegmentIterator} class.
 */
public class TableSegmentIteratorTests {
    @Test
    public void testIterator() {
        val items = new TreeMap<ByteBuf, ByteBuf>();
        for (int i = 0; i < 256; i++) {
            val value = Unpooled.wrappedBuffer(new byte[]{(byte) i});
            items.put(value, value);
        }

        val initialArgs = new SegmentIteratorArgs(items.firstKey(), items.lastKey(), 2);
        val tsi = new TableSegmentIterator<>(
                args -> CompletableFuture.completedFuture(new IteratorItem<>(
                        items.subMap(args.getFromKey(), true, args.getToKey(), true)
                                .keySet().stream()
                                .limit(args.getMaxItemsAtOnce())
                                .collect(Collectors.toList()))),
                r -> r,
                initialArgs);

        val result = new ArrayList<ByteBuf>();
        tsi.collectRemaining(i -> {
            Assert.assertEquals(initialArgs.getMaxItemsAtOnce(), i.getItems().size());
            result.addAll(i.getItems());
            return true;
        }).join();

        val expectedResult = new ArrayList<>(items.keySet());
        AssertExtensions.assertListEquals("Unexpected result.", expectedResult, result, ByteBuf::equals);
        Assert.assertNull("Not expecting any more items.", tsi.getNext().join());
    }

    @Test
    public void testSegmentIteratorArgs() {
        val fromKey = Unpooled.wrappedBuffer(new byte[]{0, 0, 0});
        val toKey = Unpooled.wrappedBuffer(new byte[]{3, (byte) 0xFF, 94});

        // We calculate the number of iterations we'd need in order to get from fromKey to toKey by incrementing one bit at a time.
        val expectedCount = 3 * 256 * 256 + 255 * 256 + 94 + 1; // We add 1 to account for the last iteration (firstKey==lastKey).

        SegmentIteratorArgs args = new SegmentIteratorArgs(fromKey, toKey, 1);
        Assert.assertNull(args.next(null)); // Check the end-of-iteration.

        int count = 0;
        while (args != null) {
            args = args.next(args.getFromKey());
            count++;
            if (count > expectedCount) {
                Assert.fail("Too many iterations."); // Just in case this ends up in an infinite loop...
            }
        }

        Assert.assertEquals(expectedCount, count);
    }
}
