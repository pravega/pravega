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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        Assert.assertTrue(timesCalled.get() >= 4);
        assertEquals(25, foundMap.size());
        Assert.assertTrue(foundMap.entrySet().stream().allMatch(x -> x.getValue() == 1));
        // endregion

        // region concurrent calls
        CompletableFuture<Void> latch = new CompletableFuture<>();
        CompletableFuture<String> latch2 = new CompletableFuture<>();
        CompletableFuture<String> latch3 = new CompletableFuture<>();
        LinkedBlockingQueue<CompletableFuture<String>> queue = new LinkedBlockingQueue<>();
        queue.add(latch2);
        queue.add(latch3);

        List<Integer> list = Lists.newArrayList(1, 2, 3);
        iterator = new ContinuationTokenAsyncIterator<>(s -> {
            int startIndex = Strings.isNullOrEmpty(s) ? 0 : Integer.parseInt(s);
            int endIndex = startIndex + 1;
            if (!Strings.isNullOrEmpty(s)) {
                CompletableFuture<String> poll = queue.poll();
                if (poll != null) {
                    poll.complete(s);
                }
                // block the call
                return latch.thenApply(v -> {
                    List<Integer> tmp = (startIndex >= list.size()) ? Lists.newArrayList() : list.subList(startIndex, endIndex);

                    return new AbstractMap.SimpleEntry<>("" + endIndex, tmp);
                });
            }

            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>("" + endIndex, list.subList(startIndex, endIndex)));
        }, "");

        Integer next0 = iterator.getNext().join();
        assertEquals(next0.intValue(), 1);
        assertEquals(iterator.getToken().get(), "1");

        CompletableFuture<Integer> next1 = iterator.getNext();
        // wait until first call is made.
        String token1 = latch2.join();
        CompletableFuture<Integer> next2 = iterator.getNext();
        // wait until second call is made.
        String token2 = latch3.join();
        // now we have two concurrent calls with same continuation token
        // verify that the continuation token is same
        assertEquals(token1, token2);
        assertEquals("1", iterator.getToken().get());
        // now signal for both calls to be completed
        latch.complete(null);

        // since next1 and next2 are called concurrently, there is no guarantee on their order. 
        assertEquals(next1.join() + next2.join(), 5);
        assertEquals(iterator.getToken().get(), "3");
        assertTrue(iterator.getQueue().isEmpty());

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

        BlockingAsyncIterator<Integer> iterator = new BlockingAsyncIterator<>(getIterator(toReturn, timesCalled));
        int found = 0;
        while (iterator.hasNext()) {
            Integer i = iterator.next();
            assertEquals(i.intValue(), found++);
        }

        AssertExtensions.assertThrows(NoSuchElementException.class, iterator::next);
    }
}
