/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;
import java.util.function.Function;

/**
 * TO-DO.
 */
public class NewtonianSearch {
    public static <T, R extends Comparable<R>> int newtonianSearch(Function<T, R> valueExtractor, R targetValue,
                                                                   int lowerBoundIndex, int upperBoundIndex) {
        int low = lowerBoundIndex;
        int high = upperBoundIndex;

        while (low <= high) {
            int mid = interpolate(low, high, valueExtractor.apply((T) Integer.valueOf(low)),
                    valueExtractor.apply((T) Integer.valueOf(high)), targetValue);

            R midValue = valueExtractor.apply((T) Integer.valueOf(mid));
            int cmp = midValue.compareTo(targetValue);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }

        return -(low + 1);
    }

    private static <T, R extends Comparable<R>> int interpolate(int low, int high, R lowValue, R highValue,
                                                                R targetValue) {
        return low
                + (int) (((double) (targetValue.compareTo(lowValue)) / (highValue.compareTo(lowValue))) * (high - low));
    }

}
