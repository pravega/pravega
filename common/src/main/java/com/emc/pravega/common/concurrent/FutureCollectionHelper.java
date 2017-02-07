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

import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Utility methods for handling future collections.
 */
public class FutureCollectionHelper {

    /**
     * Predicate that evaluates in future.
     *
     * @param <T> Type parameter.
     *
     */
    public interface FuturePredicate<T> {

        /**
         * Returns a CompletableFuture that will be completed with the result of the evaluation of the given argument.
         *
         * @param t The argument to be tested
         * @return a CompleteableFuture.
         */
        CompletableFuture<Boolean> apply(T t);
    }

    /**
     * Filter that takes a predicate that evaluates in future and returns the filtered list that evaluates in future.
     *
     * @param input     Input list.
     * @param predicate Predicate that evaluates in the future.
     * @param <T>       Type parameter.
     * @return a CompletableFuture that contains a Map of the results of the given Futures.
     */
    public static <T> CompletableFuture<List<T>> filter(List<T> input, FuturePredicate<T> predicate) {
        Preconditions.checkNotNull(input);

        List<CompletableFuture<Boolean>> future = input.stream().map(predicate::apply).collect(Collectors.toList());
        return sequence(future).thenApply(
                booleanList -> {
                    List<T> result = new ArrayList<>();
                    int i = 0;
                    for (T elem : input) {
                        if (booleanList.get(i)) {
                            result.add(elem);
                        }
                        i++;
                    }
                    return result;
                });
    }

    /**
     * Converts a list of futures into a future list.
     * If any of the futures in the input list completes exceptionally then the result completes exceptionally.
     * Note that although this function uses a blocking join method, the function is not blocking, because join
     * is invoked on a completed future.
     *
     * @param futures List of futures.
     * @param <T>     Type parameter.
     * @return A future list.
     */
    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v ->
                        futures.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.<T>toList())
        );
    }

    /**
     * Returns a list of all futures.
     *
     * @param futureMap List of futures.
     * @param <T> Type parameter
     * @param <U> Type parameter
     * @return A completable future list
     */
    public static <T, U> CompletableFuture<Map<T, U>> sequenceMap(Map<T, CompletableFuture<U>> futureMap) {
        return CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[futureMap.size()]))
                .thenApply(x ->
                                futureMap.entrySet().stream()
                                        .map(y -> {
                                            try {
                                                return new AbstractMap.SimpleEntry<T, U>(y.getKey(), y.getValue().get());
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );
    }
}
