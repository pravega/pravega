package com.emc.logservice.common;

/**
 * Exception that is thrown whenever a bad configuration is detected.
 */
public class ConfigurationException extends Exception {
    /**
     * Creates a new instance of the ConfigurationException class.
     * @param message The message.
     */
    public ConfigurationException(String message){
        super(message);
    }

    /**
     * Creates a new instance of the ConfigurationException class.
     * @param message The message.
     * @param cause The cause.
     */
    public ConfigurationException(String message, Throwable cause){
        super(message, cause);
    }
}
