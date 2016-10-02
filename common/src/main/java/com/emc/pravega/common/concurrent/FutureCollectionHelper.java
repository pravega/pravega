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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Utility methods for handling future collections.
 */
public class FutureCollectionHelper {

    /**
     * Predicate that evaluates in future.
     * @param <T> Type parameter.
     */
    public interface FuturePredicate<T> {
        CompletableFuture<Boolean> apply(T t);
    }

    /**
     * Filter that takes a predicate that evaluates in future and returns the filtered list that evaluates in future.
     * @param input Input list.
     * @param predicate Predicate that evaluates in the future.
     * @param <T> Type parameter.
     * @return List that evaluates in future.
     */
    public static <T> CompletableFuture<List<T>> filter(List<T> input, FuturePredicate<T> predicate) {
        Preconditions.checkNotNull(input);

        List<CompletableFuture<Boolean>> future = new ArrayList<>();
        for (T elem: input) {
            future.add(predicate.apply(elem));
        }
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
     * List aggregator that applies an aggregator function that evaluates in future on a list that evaluates in future
     * and generates the aggregate value that evaluates in future.
     * @param input Input list that evaluates in future.
     * @param zero Initial aggregate value.
     * @param fn Aggregator that evaluates in future.
     * @param <T> Input list type parameter.
     * @param <U> Aggregated output's type parameter.
     * @return Aggregate that evaluates in future.
     */
    public static <T, U> CompletableFuture<U> foldFutures(CompletableFuture<List<T>> input, U zero, BiFunction<CompletableFuture<U>, ? super T, CompletableFuture<U>> fn) {
        Preconditions.checkNotNull(input);
        CompletableFuture<U> zeroFuture = new CompletableFuture<>();
        zeroFuture.complete(zero);
        return input.thenCompose(list -> {
            CompletableFuture<U> current = zeroFuture;
            for (T elem : list) {
                current = fn.apply(current, elem);
            }
            return current;
        });
    }

    /**
     * List aggregator that applies an aggregator function that evaluates in future on a list and generates
     * the aggregate value that evaluates in future.
     * @param input Input list.
     * @param zero Initial aggregate value.
     * @param fn Aggregator that evaluates in future.
     * @param <T> Input list type parameter.
     * @param <U> Aggregated output's type parameter.
     * @return Aggregate that evaluates in future.
     */
    public static <T, U> CompletableFuture<U> foldFutures(List<T> input, CompletableFuture<U> zero, BiFunction<CompletableFuture<U>, ? super T, CompletableFuture<U>> fn) {
        Preconditions.checkNotNull(input);
        CompletableFuture<U> current = zero;
        for (T elem: input) {
            current = fn.apply(current, elem);
        }
        return current;
    }

    /**
     * Converts a list of futures into a future list.
     * Note that although this function uses a blocking join method, the function is not blocking,
     * because join is invoked on a completed future.
     * @param futures List of futures.
     * @param <T> Type parameter.
     * @return A future list.
     */
    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v ->
                        futures.stream().
                                map(future -> future.join()).
                                collect(Collectors.<T>toList())
        );
    }

}
