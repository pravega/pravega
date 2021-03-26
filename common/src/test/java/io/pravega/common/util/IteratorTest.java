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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class IteratorTest {
    @Test(timeout = 10000L)
    public void testContinuationTokenIterator() {
        List<Integer> toReturn = IntStream.range(0, 25).boxed().collect(Collectors.toList());

        AtomicInteger timesCalled = new AtomicInteger(0);
        ContinuationTokenAsyncIterator<String, Integer> iterator = getIterator(toReturn, timesCalled);

        // region sequential calls
        Integer next = iterator.getNext().join();
        int found = 0;
        while (next != null) {
            assertEquals(next.intValue(), found++);
            next = iterator.getNext().join();
        }
        assertEquals(4, timesCalled.get());
        assertEquals(25, found);
        // endregion

        // region concurrent calls
        timesCalled = new AtomicInteger(0);
        iterator = getIterator(toReturn, timesCalled);

        ConcurrentHashMap<Integer, Integer> foundMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        for (int i = 0; i < 26; i++) {
            futures.add(iterator.getNext().thenAccept(x -> {
                if (x != null) {
                    foundMap.compute(x, (r, s) -> {
                        if (s != null) {
                            return s + 1;
                        } else {
                            return 1;
                        }
                    });
                }
            }));
        }

        Futures.allOf(futures).join();

        Assert.assertEquals(4, timesCalled.get());
        assertEquals(25, foundMap.size());
        Assert.assertTrue(foundMap.entrySet().stream().allMatch(x -> x.getValue() == 1));
        // endregion

        // region concurrent calls
        CompletableFuture<Void> latch = new CompletableFuture<>();

        List<Integer> list = Lists.newArrayList(1, 2, 3);
        AtomicInteger functionCalledCount = new AtomicInteger(0);
        iterator = spy(new ContinuationTokenAsyncIterator<>(s -> {
            functionCalledCount.incrementAndGet();
            int startIndex = Strings.isNullOrEmpty(s) ? 0 : Integer.parseInt(s);
            int endIndex = startIndex + 1;
            if (!Strings.isNullOrEmpty(s)) {
                // block the call
                return latch.thenApply(v -> {
                    List<Integer> tmp = (startIndex >= list.size()) ? Lists.newArrayList() : list.subList(startIndex, endIndex);

                    return new AbstractMap.SimpleEntry<>("" + endIndex, tmp);
                });
            }

            CompletableFuture<Map.Entry<String, Collection<Integer>>> completedFuture = CompletableFuture.completedFuture(new AbstractMap.SimpleEntry<String, Collection<Integer>>(""
                    + endIndex, list.subList(startIndex, endIndex)));
            return completedFuture;
        }, ""));

        Integer next0 = iterator.getNext().join();
        assertEquals(next0.intValue(), 1);
        assertEquals(1, functionCalledCount.get());
        assertEquals(iterator.getToken(), "1");

        CompletableFuture<Integer> next1 = iterator.getNext();
        // wait until first call is made.

        assertEquals("1", iterator.getToken());

        CompletableFuture<Integer> next2 = iterator.getNext();
        // this should keep calling getNext in a loop until it gets the value. 
        // verify that iterator.getNext is called multiple times.
        verify(iterator, atLeast(3)).getNext();
        
        assertEquals(2, functionCalledCount.get());
        
        // signal to complete getNext with token "1"
        latch.complete(null);
        
        // since next1 and next2 are called concurrently, there is no guarantee on their order. 
        assertEquals(next1.join() + next2.join(), 5);
        assertEquals(3, functionCalledCount.get());

        assertEquals(iterator.getToken(), "3");
        assertTrue(iterator.isInternalQueueEmpty());

        assertNull(iterator.getNext().join());
        // endregion

    }

    private ContinuationTokenAsyncIterator<String, Integer> getIterator(List<Integer> toReturn, AtomicInteger timesCalled) {
        return new ContinuationTokenAsyncIterator<>(s -> {
            timesCalled.incrementAndGet();
            int startIndex = Strings.isNullOrEmpty(s) ? 0 : Integer.parseInt(s);
            if (startIndex >= toReturn.size()) {
                return CompletableFuture.completedFuture(new AbstractMap.SimpleEntry<>(s, Lists.newArrayList()));
            }
            int endIndex = startIndex + 10;
            endIndex = endIndex > toReturn.size() ? toReturn.size() : endIndex;
            return CompletableFuture.completedFuture(new AbstractMap.SimpleEntry<>("" + endIndex, toReturn.subList(startIndex, endIndex)));
        }, "");
    }

    @Test(timeout = 10000L)
    public void testBlockingIterator() {
        List<Integer> toReturn = IntStream.range(0, 25).boxed().collect(Collectors.toList());
        AtomicInteger timesCalled = new AtomicInteger(0);

        Iterator<Integer> iterator = getIterator(toReturn, timesCalled).asIterator();
        int found = 0;
        while (iterator.hasNext()) {
            Integer i = iterator.next();
            assertEquals(i.intValue(), found++);
        }

        AssertExtensions.assertThrows(NoSuchElementException.class, iterator::next);
    }
}
