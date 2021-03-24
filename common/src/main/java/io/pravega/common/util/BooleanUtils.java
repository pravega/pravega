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

import java.util.Optional;

public class BooleanUtils {

    /**
     * Extracts an optional boolean from a String {@code value}.
     *
     * @param value the value to extract the boolean from
     * @return an {@link Optional} instance wrapping a {@link Boolean} instance, if {@code value} isn't null, empty or
     * a non-boolean string. Otherwise, returns {@code Optional.empty()}.
     */
    public static Optional<Boolean> extract(String value) {
        if (value == null || value.trim().equals("")) {
            return Optional.empty();
        }

        String trimmedValue = value.trim();
        if (trimmedValue.equalsIgnoreCase("yes")
                || trimmedValue.equalsIgnoreCase("y")
                || trimmedValue.equalsIgnoreCase("true")) {
            return Optional.of(true);
        } else if (trimmedValue.equalsIgnoreCase("no")
                || trimmedValue.equalsIgnoreCase("n")
                || trimmedValue.equalsIgnoreCase("false")) {
            return Optional.of(false);
        } else {
            return Optional.empty();
        }
    }
}
