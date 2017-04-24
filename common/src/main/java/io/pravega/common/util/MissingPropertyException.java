/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.util;

/**
 * Exception that is thrown whenever a required Configuration Property is Missing.
 */
public class MissingPropertyException extends ConfigurationException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the MissingPropertyException class.
     * @param fullPropertyName The full name (component + property name) of the property.
     */
    public MissingPropertyException(String fullPropertyName) {
        super(String.format("Could not find property '%s'.", fullPropertyName));
    }
}
