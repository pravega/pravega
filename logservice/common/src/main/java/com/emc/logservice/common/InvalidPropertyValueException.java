package com.emc.logservice.common;

/**
 * Exception that is thrown whenever a Property Value is invalid based on what is expected.
 */
public class InvalidPropertyValueException extends ConfigurationException {
    /**
     * Creates a new instance of the InvalidPropertyValueException class.
     *
     * @param fullPropertyName The full name (component + property) of the property.
     * @param actualValue      The actual value that was about to be processed.
     */
    public InvalidPropertyValueException(String fullPropertyName, String actualValue) {
        super(String.format("Value '%s' is invalid for property '%s'.", actualValue, fullPropertyName));
    }

    /**
     * Creates a new instance of the InvalidPropertyValueException class.
     *
     * @param fullPropertyName The full name (component + property) of the property.
     * @param actualValue      The actual value that was about to be processed.
     * @param cause            The causing Exception for this.
     */
    public InvalidPropertyValueException(String fullPropertyName, String actualValue, Throwable cause) {
        super(String.format("Value '%s' is invalid for property '%s'.", actualValue, fullPropertyName), cause);
    }
}
