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

package com.emc.pravega.common.util;

import com.emc.pravega.common.function.ConsumerWithException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Helper methods for collection.
 */
public class CollectionHelpers {
    /**
     * Creates a new Collection containing only those elements in the given collection that match the given filter.
     *
     * @param collection The collection to use.
     * @param filter     The filter to apply.
     * @param <T>        The type of the elements in the collection.
     */
    public static <T> Collection<T> filter(Collection<T> collection, Predicate<T> filter) {
        ArrayList<T> result = new ArrayList<>();
        for (T element : collection) {
            if (filter.test(element)) {
                result.add(element);
            }
        }

        return result;
    }

    /**
     * Applies the given ConsumerWithException to each element in the given collection.
     *
     * @param collection The collection to use.
     * @param processor  The element processor.
     * @param <T>        The type of the elements in the collection.
     * @param <TEX>      The type of exceptions to expect.
     * @throws TEX If the processor threw one.
     */
    public static <T, TEX extends Throwable> void forEach(Collection<T> collection,
                                           ConsumerWithException<T, TEX> processor) throws TEX {
        for (T element : collection) {
            processor.accept(element);
        }
    }

    /**
     * Applies the given ConsumerWithException to each element in the given collection.
     *
     * @param collection The collection to use.
     * @param filter     The filter to apply.
     * @param processor  The element processor.
     * @param <T>        The type of the elements in the collection.
     * @param <TEX>      The type of exceptions to expect.
     * @throws TEX If the processor threw one.
     */
    public static <T, TEX extends Throwable> void forEach(Collection<T> collection, Predicate<T> filter,
                                                          ConsumerWithException<T, TEX> processor) throws TEX {
        for (T element : collection) {
            if (filter.test(element)) {
                processor.accept(element);
            }
        }
    }
}
