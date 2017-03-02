/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import java.util.Map;
import java.util.Properties;

/**
 * Extension to java.util.Properties that implements the Builder pattern.
 */
public class PropertyBag extends Properties {

    private PropertyBag() {
    }

    /**
     * Creates a new instance of the PropertyBag class.
     * @return A new, empty instance of the PropertyBag class.
     */
    public static PropertyBag create() {
        return new PropertyBag();
    }

    /**
     * Creates a new instance of the PropertyBag class.
     *
     * @param base A Properties object to clone.
     * @return A new instance of the PropertyBag class with the contents of the given Properties.
     */
    public static PropertyBag create(Properties base) {
        PropertyBag result = new PropertyBag();
        for (Map.Entry<Object, Object> e : base.entrySet()) {
            result.setProperty((String) e.getKey(), (String) e.getValue());
        }

        return result;
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
