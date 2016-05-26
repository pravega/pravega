package com.emc.logservice.common;

/**
 * Exception that is thrown whenever a required Configuration Property is Missing.
 */
public class MissingPropertyException extends ConfigurationException {
    /**
     * Creates a new instance of the MissingPropertyException class.
     * @param fullPropertyName The full name (component + property name) of the property.
     */
    public MissingPropertyException(String fullPropertyName) {
        super(String.format("Could not find property '%s'.", fullPropertyName));
    }
}
