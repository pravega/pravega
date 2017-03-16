/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Property that can be used for configuration.
 */
@RequiredArgsConstructor
public class Property<T> {
    @Getter
    private final String name;
    @Getter
    private final T defaultValue;

    /**
     * Creates a new instance of the Property class with no default value.
     * @param name The name of the property.
     */
    public Property(String name) {
        this(name, null);
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
