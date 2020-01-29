/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

    /**
     * Creates a new instance of the Property class with no default value.
     *
     * @param name The name of the property.
     * @param <T>  The type of the property values.
     * @return A new instance of the Property class with no default value.
     */
    public static <T> Property<T> named(String name) {
        return new Property<>(name, null);
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
        return new Property<>(name, defaultValue);
    }

    /**
     * Gets a value indicating whether this Property has a default value.
     */
    boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
