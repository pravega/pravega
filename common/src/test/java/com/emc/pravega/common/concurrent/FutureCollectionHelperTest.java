/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.emc.pravega.common.concurrent.FutureCollectionHelper.*;

/**
 * Test methods for FutureCollectionHelpers.
 */
public class FutureCollectionHelperTest {

    /**
     * Test method for FutureCollectionHelpers.filter.
     *
     * @throws InterruptedException when future is interrupted
     * @throws ExecutionException   when future is interrupted
     */
    @Test
    public void testFilter() throws ExecutionException, InterruptedException {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        Predicate<Integer> evenFilter = (Integer x) -> x % 2 == 0;
        FuturePredicate<Integer> futureEvenFilter = (Integer x) -> CompletableFuture.completedFuture(x % 2 == 0);

        CompletableFuture<List<Integer>> filteredList = filter(list, futureEvenFilter);

        Assert.assertEquals(filteredList.get().size(), 3);
        Assert.assertEquals(filteredList.get(), list.stream().filter(evenFilter).collect(Collectors.toList()));
    }

    /**
     * Test method for FutureCollectionHelpers.filter when the FuturePredicate completes exceptionally in future.
     */
    @Test(expected = CompletionException.class)
    public void testFilterException() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        FuturePredicate<Integer> futureEvenFilter = (Integer x) -> CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });

        CompletableFuture<List<Integer>> filteredList = filter(list, futureEvenFilter);

        filteredList.join();
    }

    /**
     * Test method for FutureCollectionHelpers.sequence.
     *
     * @throws ExecutionException   when future is interrupted
     * @throws InterruptedException when future is interrupted
     */
    @Test
    public void testSequence() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Integer>> list = new ArrayList<>();

        int n = 10;
        for (int i = 0; i < n; i++) {
            CompletableFuture<Integer> x = new CompletableFuture<>();
            x.complete(i);
            list.add(x);
        }

        CompletableFuture<List<Integer>> sequence = sequence(list);

        List<Integer> returnList = sequence.get();
        Assert.assertEquals(returnList.size(), n);
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(returnList.get(i).intValue(), i);
        }
    }

    /**
     * Test method for FutureCollectionHelpers.sequence when some elements in original
     * list complete exceptionally in future.
     */
    @Test(expected = CompletionException.class)
    public void testSequenceException() {
        List<CompletableFuture<Integer>> list = new ArrayList<>();

        int n = 10;
        for (int i = 0; i < n; i++) {
            CompletableFuture<Integer> x = new CompletableFuture<>();
            if (i % 2 == 1) {
                x.completeExceptionally(new RuntimeException());
            } else {
                x.complete(i);
            }
            list.add(x);
        }

        CompletableFuture<List<Integer>> sequence = sequence(list);

        sequence.join();
    }
}
