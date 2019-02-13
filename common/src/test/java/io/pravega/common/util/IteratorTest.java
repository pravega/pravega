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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IteratorTest {
    @Test
    public void testContinuationTokenIterator() {
        List<Integer> toReturn = IntStream.range(0, 25).boxed().collect(Collectors.toList());

        AtomicInteger timesCalled = new AtomicInteger(0);
        ContinuationTokenAsyncIterator<String, Integer> iterator = getIterator(toReturn, timesCalled);
        
        // region sequential calls
        Integer next = iterator.getNext().join();
        int found = 0;
        while(next != null) {
            Assert.assertEquals(next.intValue(), found++);
            next = iterator.getNext().join();
        }
        Assert.assertEquals(4, timesCalled.get());
        Assert.assertEquals(25, found);
        // endregion
        
        // region concurrent calls
        timesCalled = new AtomicInteger(0);
        iterator = getIterator(toReturn, timesCalled);
        
        ConcurrentHashMap<Integer, Integer> foundMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        for(int i = 0; i < 26; i++) {
            futures.add(iterator.getNext().thenAccept(x -> {
                if (x != null) {
                    foundMap.compute(x, (r, s) -> {
                        if (s != null) {
                            return ++s;
                        } else {
                            return 1;
                        }
                    });
                } 
            }));
        }

        Futures.allOf(futures).join();
        
        Assert.assertEquals(4, timesCalled.get());
        Assert.assertEquals(25, foundMap.size());
        Assert.assertTrue(foundMap.entrySet().stream().allMatch(x -> x.getValue() == 1));
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
    
    @Test
    public void testBlockingIterator() {
        List<Integer> toReturn = IntStream.range(0, 25).boxed().collect(Collectors.toList());
        AtomicInteger timesCalled = new AtomicInteger(0);

        BlockingAsyncIterator<Integer> iterator = new BlockingAsyncIterator<>(getIterator(toReturn, timesCalled));
        int found = 0;
        while(iterator.hasNext()) {
            Integer i = iterator.next();
            Assert.assertEquals(i.intValue(), found++);
        }

        AssertExtensions.assertThrows(NoSuchElementException.class, iterator::next);
    }
}
