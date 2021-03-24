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

import com.google.common.collect.ImmutableList;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        val orderedItems = ImmutableList.<Map.Entry<IteratorState, List<Integer>>>builder()
                .add(new AbstractMap.SimpleImmutableEntry<>(newIteratorState(), Arrays.asList(1, 2, 3)))
                .add(new AbstractMap.SimpleImmutableEntry<>(newIteratorState(), Arrays.asList(4, 5, 6)))
                .add(new AbstractMap.SimpleImmutableEntry<>(newIteratorState(), Arrays.asList(7, 8, 9)))
                .build();
        val iteratorItems = new HashMap<IteratorState, Integer>();
        for (int i = 0; i < orderedItems.size(); i++) {
            iteratorItems.put(orderedItems.get(i).getKey(), i);
        }

        val tsi = new TableSegmentIterator<Integer>(
                s -> CompletableFuture.completedFuture(getIteratorItem(s, iteratorItems, orderedItems)),
                null);

        val result = new ArrayList<Integer>();
        tsi.collectRemaining(i -> result.addAll(i.getItems()));

        val expectedResult = orderedItems.stream().flatMap(i -> i.getValue().stream()).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected result.", expectedResult, result, Integer::equals);

        Assert.assertNull("Not expecting any more items.", tsi.getNext().join());
    }

    private IteratorItem<Integer> getIteratorItem(IteratorState state, Map<IteratorState, Integer> iteratorItems,
                                                  List<Map.Entry<IteratorState, List<Integer>>> orderedItems) {
        int index = state == null ? 0 : iteratorItems.getOrDefault(state, -1);
        if (index < 0) {
            return null;
        }

        val contents = orderedItems.get(index).getValue();
        val nextState = index < orderedItems.size() - 1 ? orderedItems.get(index + 1).getKey() : IteratorStateImpl.EMPTY;
        return new IteratorItem<>(nextState, contents);
    }

    private IteratorState newIteratorState() {
        byte[] contents = new byte[10];
        return IteratorState.fromBytes(ByteBuffer.wrap(contents));
    }


}
