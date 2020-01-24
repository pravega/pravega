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
