/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

/**
 * Exception that is thrown whenever a bad configuration is detected.
 */
public class ConfigurationException extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the ConfigurationException class.
     * @param message The message.
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the ConfigurationException class.
     * @param message The message.
     * @param cause The cause.
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
