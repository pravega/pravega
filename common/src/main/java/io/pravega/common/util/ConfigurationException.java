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
