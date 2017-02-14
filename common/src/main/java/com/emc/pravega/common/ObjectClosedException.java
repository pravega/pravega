/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common;

/**
 * Thrown when an object has been closed via AutoCloseable.close().
 */
public class ObjectClosedException extends IllegalStateException {

    /**
     * Verion Id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of ObjectClosedException class.
     *
     * @param object The object that has been closed but tried to get accessed
     */
    public ObjectClosedException(Object object) {
        super(getMessage(object));
    }

    /**
     * Creates a new instance of ObjectClosedException class.
     *
     * @param object The object that has been closed but tried to get accessed
     * @param cause Throwable that describes the cause of exception
     */
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
