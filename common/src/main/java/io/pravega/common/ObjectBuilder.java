/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

/**
 * Defines an object that can build other objects. Usually this is a shadow object that accumulates all the data for an
 * immutable object, and it will instantiate such object with the accumulated data.
 *
 * @param <T> Type of the object to build.
 */
public interface ObjectBuilder<T> {
    /**
     * Creates a new instance of the T type (usually with the accumulated data so far).
     *
     * @return A new instance.
     */
    T build();
}
