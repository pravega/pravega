/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import java.util.List;
import java.util.function.Function;

/**
 * Helper methods for collections.
 */
public final class Collections {

    /**
     * Performs a binary search on the given sorted list using the given comparator.
     * This method has undefined behavior if the list is not sorted.
     * <p>
     * This method is different than that in java.util.Collections in that this one searches by a simple comparator, vs
     * the ones in that class which search for a specific element. This one is useful if we don't have an instance of a
     * search object or we want to implement a fuzzy comparison.
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
}
