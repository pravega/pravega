/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import java.util.Properties;

/**
 * Extension to java.util.Properties that implements the Builder pattern.
 */
public class PropertyBag extends Properties {

    private PropertyBag() {
    }

    /**
     * Creates a new instance of the PropertyBag class.
     */
    public static PropertyBag create() {
        return new PropertyBag();
    }

    /**
     * Adds the given key-value to the PropertyBag.
     *
     * @param key   The key to add.
     * @param value The value to add.
     * @return A pointer to this object.
     */
    public PropertyBag with(String key, Object value) {
        setProperty(key, value.toString());
        return this;
    }
}
