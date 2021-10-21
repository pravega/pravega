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

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.ToIntFunction;

/**
 * General helpers on Enum types.
 */
public final class EnumHelpers {
    /**
     * Indexes the values of the given Enum by a specified Id function. This method maps getId on an integer interval
     * from 0 to max()+1, and fills in the slots where getId returns a value. Empty slots will contain null.
     * <p>
     * Notes:
     * <ul>
     * <li> This method will create an array of size max(getId(all-enum-values)), so it could get very large. It is not
     * recommended for sparse Enums (in which case a Map would be better suited).
     * <li> This method has undefined behavior if getId does not return unique values. It does not check, so Enum values
     * that have the same result from getId may not be indexed correctly.
     * </ul>
     *
     * @param enumClass The Enum Class.
     * @param getId     A function that returns an Id for each enum value.
     * @param <T>       Type of the enum.
     * @return An array of type T with Enum values indexed by the result of the getId function.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> T[] indexById(Class<T> enumClass, ToIntFunction<T> getId) {
        Preconditions.checkArgument(enumClass.isEnum(), "Given class is not an enum.");
        T[] values = enumClass.getEnumConstants();
        T[] result = (T[]) Array.newInstance(enumClass, Arrays.stream(values).mapToInt(getId).max().orElse(0) + 1);
        for (T value : values) {
            result[getId.applyAsInt(value)] = value;
        }

        return result;
    }
}
