/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

/**
 * Thrown when an object has been closed via AutoCloseable.close().
 */
public class ObjectClosedException extends IllegalStateException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ObjectClosedException(Object object) {
        super(getMessage(object));
    }

    public ObjectClosedException(Object object, Throwable cause) {
        super(getMessage(object), cause);
    }

    private static String getMessage(Object object) {
        if (object == null) {
            return "Object has been closed and cannot be accessed anymore.";
        } else {
            return String.format("Object '%s' has been closed and cannot be accessed anymore.", object.getClass().getSimpleName());
        }
    }
}
