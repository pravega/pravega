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
package com.emc.pravega.controller.store.stream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * StoreException is a high level exception thrown when an exception arises from Stream Store.
 */
@Slf4j
@Getter
public class StoreException extends RuntimeException {

    /**
     * Enum to describe the type of exception.
     */
    public enum Type {
        NODE_EXISTS,
        NODE_NOT_FOUND,
        NODE_NOT_EMPTY,
        UNKNOWN
    }

    private final Type type;

    /**
     * Constructs a new store exception with the specified type and cause of exception.
     *
     * @param type Type of exception.
     * @param e    Actual cause of Exception.
     */
    public StoreException(final Type type, Exception e) {
        super(e);
        this.type = type;
    }

    /**
     * Constructs a new store exception with the specified cause and message.
     *
     * @param type    Type of exception.
     * @param message Message to describe the type of exception.
     */
    public StoreException(final Type type, final String message) {
        super(message);
        this.type = type;
    }
}
