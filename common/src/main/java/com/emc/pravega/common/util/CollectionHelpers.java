/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper methods for collections.
 */
public final class CollectionHelpers {

    /**
     * Performs a binary search on the given sorted list using the given comparator.
     * This method has undefined behavior if the list is not sorted.
     * <p>
     * This method is different than that in java.util.Collections in the following ways:
     * 1. This one searches by a simple comparator, vs the ones in the Collections class which search for a specific element.
     * This one is useful if we don't have an instance of a search object or we want to implement a fuzzy comparison.
     * 2. This one returns -1 if the element is not found. The ones in the Collections class return (-(start+1)), which is
     * the index where the item should be inserted if it were to go in the list.
     *
     * @param list       The list to search on.
     * @param comparator The comparator to use for comparison. Returns -1 if sought item is before the current item,
     *                   +1 if it is after or 0 if an exact match.
     * @param <T>        Type of the elements in the list.
     * @return The index of the sought item, or -1 if not found.
     */
    public static <T> int binarySearch(List<? extends T> list, Function<? super T, Integer> comparator) {
        int start = 0;
        int end = list.size() - 1;

        while (start <= end) {
            int midIndex = start + end >>> 1;
            T midElement = list.get(midIndex);
            int compareResult = comparator.apply(midElement);
            if (compareResult < 0) {
                end = midIndex - 1;
            } else if (compareResult > 0) {
                start = midIndex + 1;
            } else {
                return midIndex;
            }
        }

        return -1;
    }

    /**
     * Returns a new collection which contains all the items in the given collection that are not to be excluded.
     *
     * @param collection The collection to check.
     * @param toExclude  The elements to exclude.
     * @param <T>        Type of elements.
     * @return A new collection containing all items in collection, except those in toExclude.
     */
    public static <T> Collection<T> filterOut(Collection<T> collection, Collection<T> toExclude) {
        return collection.stream().filter(o -> !toExclude.contains(o)).collect(Collectors.toList());
    }
}
