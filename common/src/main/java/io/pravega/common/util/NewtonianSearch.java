/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import java.util.List;

/**
 * TO-DO.
 */
public class NewtonianSearch {
    @FunctionalInterface
    public interface DensityFunction<T> {
        double getDensity(T item);
    }

    @SuppressWarnings("unchecked")
    public static <T> int newtonianSearch(List<? extends Comparable<? super T>> list,
                                          DensityFunction<T> densityFunction, T key) {
        int lowerBound = 0;
        int upperBound = list.size() - 1;

        while (lowerBound <= upperBound) {
            int mid = (lowerBound + upperBound) >>> 1;
            Comparable<? super T> midVal = list.get(mid);
            double midDensity = densityFunction.getDensity((T) midVal);
            double keyDensity = densityFunction.getDensity(key);

            if (midDensity < keyDensity) {
                lowerBound = mid + 1;
            } else if (midDensity > keyDensity) {
                upperBound = mid - 1;
            } else {
                return mid; // Key found
            }
        }
        return -(lowerBound + 1); // Key not found
    }
}
