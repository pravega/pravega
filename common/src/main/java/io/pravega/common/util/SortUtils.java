
/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.MathHelpers;
import java.util.AbstractMap;
import java.util.Map;
import java.util.function.LongFunction;

public class SortUtils {

    /**
     * Newtonian search: Identical to binary search, but specialized to searching lists of longs.
     * The difference is in how the midpoint is chosen. In a standard binary search, the middle
     * element is always chosen for the guess. In Newtonian search, the midpoint is chosen to be the
     * index at the percentile the target element would be if the list items were uniformly spaced.
     * For example, if a list were provided with a minimum element of 0 and a max of 100 and the
     * target value of 70, then the midpoint would be chosen to be 70% of the way through the list.
     *
     * @param getValue The function that returns a long given an index
     * @param fromIdx The index from which to start the search
     * @param toIdx The index at which to end the search
     * @param target The value to search for
     * @return an Entry object containing the closest index and its value
     * @throws IllegalArgumentException if the input list is empty
     */
    public static Map.Entry<Integer, Long> newtonianSearch(LongFunction<Long> getValue, int fromIdx, int toIdx,
                                                           long target) {
        if (fromIdx > toIdx || fromIdx < 0) {
            throw new IllegalArgumentException("Index size was negative");
        } else if (fromIdx == toIdx) {
            return new AbstractMap.SimpleEntry<>(fromIdx, getValue.apply(fromIdx));
        }

        long fromValue = getValue.apply(fromIdx);
        long toValue = getValue.apply(toIdx);

        if (target <= fromValue) {
            return new AbstractMap.SimpleEntry<>(fromIdx, fromValue);
        }
        if (target >= toValue) {
            return new AbstractMap.SimpleEntry<>(toIdx, toValue);
        }
        double beginSlope = calculateSlope(fromIdx, toIdx, fromValue, toValue); 
        double endSlope = beginSlope;
        while (toIdx > fromIdx + 1) {
            double guessProportion = ((double) target - (double) fromValue) / ((double) toValue - (double) fromValue);
            double slope = (1.0 - guessProportion) * beginSlope + (guessProportion) * endSlope;
            int guessIdx;
            if (guessProportion < 0.5) {
                guessIdx = fromIdx + (int) (((double) (target - fromValue)) / slope);
            } else {
                guessIdx = toIdx - (int) (((double) (toValue - target)) / slope);
            }
            guessIdx = MathHelpers.minMax(guessIdx, fromIdx + 1, toIdx - 1);

            long guessValue = getValue.apply(guessIdx);
            beginSlope = calculateSlope(fromIdx, guessIdx, fromValue, guessValue);
            endSlope = calculateSlope(guessIdx, toIdx, guessValue, toValue);

            if (guessValue < target) {
                fromIdx = guessIdx;
                fromValue = guessValue;
            } else if (guessValue > target) {
                toIdx = guessIdx;
                toValue = guessValue;
            } else {
                return new AbstractMap.SimpleEntry<>(guessIdx, guessValue);
            }
        }
        return new AbstractMap.SimpleEntry<>(fromIdx, fromValue);
    }

    private static double calculateSlope(int fromIdx, int toIdx, long fromValue, long toValue) {
        //Divide and multiply by 2 to prevent wrapping issues for large values.
        return 2 * ((double) (toValue/2 - fromValue/2) / (double) (toIdx - fromIdx));
    }

    /**
     * Binary search over an external collection of longs.
     * This is mainly a tests function for reference. Use {@link #newtonianSearch(LongFunction, int, int, long)}
     * If there is an approximately uniform or slowly changing value density.
     *
     * @param getValue The function that returns a long given an index
     * @param fromIdx The index from which to start the search
     * @param toIdx The index at which to end the search
     * @param target The value to search for
     * @return an Entry object containing the closest index and its value
     * @throws IllegalArgumentException if the input list is empty
     */
    @VisibleForTesting
    static Map.Entry<Integer, Long> binarySearch(LongFunction<Long> getValue, int fromIdx, int toIdx,
                                                        long target) {
        if (fromIdx > toIdx) {
            throw new IllegalArgumentException("Index size was negitive");
        } else if (fromIdx == toIdx) {
            return new AbstractMap.SimpleEntry<>(fromIdx, getValue.apply(fromIdx));
        }
        long fromValue = getValue.apply(fromIdx);
        long toValue = getValue.apply(toIdx);

        if (target <= fromValue) {
            return new AbstractMap.SimpleEntry<>(fromIdx, fromValue);
        }
        if (target >= toValue) {
            return new AbstractMap.SimpleEntry<>(toIdx, toValue);
        }

        while (toIdx > fromIdx + 1) {
            int mid = fromIdx + (toIdx - fromIdx) / 2;
            long midValue = getValue.apply(mid);

            if (midValue < target) {
                fromIdx = mid;
                fromValue = midValue;
            } else if (midValue > target) {
                toIdx = mid;
                toValue = midValue;
            } else {
                return new AbstractMap.SimpleEntry<>(mid, midValue);
            }
        }
        return new AbstractMap.SimpleEntry<>(fromIdx, fromValue);
    }

}
