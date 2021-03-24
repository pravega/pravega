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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Property that can be used for configuration.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class Property<T> {
    @Getter
    private final String name;
    @Getter
    private final T defaultValue;
    @Getter
    private final String legacyName;

    /**
     * Creates a new instance of the Property class with no default value.
     *
     * @param name The name of the property.
     * @param <T>  The type of the property values.
     * @return A new instance of the Property class with no default value.
     */
    public static <T> Property<T> named(String name) {
        return new Property<>(name, null, null);
    }

    /**
     * Creates a new instance of the Property class with the given default value.
     *
     * @param name         The name of the property.
     * @param defaultValue The default value of the property.
     * @param <T>          The type of the property values.
     * @return A new instance of the Property class with the given default value.
     */
    public static <T> Property<T> named(String name, T defaultValue) {
        return new Property<>(name, defaultValue, null);
    }

    /**
     * Creates a new instance of the Property class with the given default value.
     *
     * @param name          The name of the property.
     * @param defaultValue  The default value of the property.
     * @param legacyName    The old name of the property, for backward compatibility.
     * @param <T>           The type of the property values.
     * @return A new instance of the Property class with the given default value.
     */
    public static <T> Property<T> named(String name, T defaultValue, String legacyName) {
        return new Property<>(name, defaultValue, legacyName);
    }

    /**
     * Gets a value indicating whether this Property has a default value.
     */
    boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    /**
     * Determines whether this instance has a legacy name.
     *
     * @return {@code true} if this has a legacy name, otherwise {@code false}.
     */
    boolean hasLegacyName() {
        return this.legacyName != null;
    }

    /**
     * Returns full property name using the specified component code.
     *
     * @param componentCode the component prefix (`controller.`, `pravegaservice`, etc.)
     * @return fully qualified property name
     */
    public String getFullName(String componentCode) {
        return String.format("%s.%s", componentCode, this.getName());
    }

    @Override
    public String toString() {
        return this.name;
    }
}
