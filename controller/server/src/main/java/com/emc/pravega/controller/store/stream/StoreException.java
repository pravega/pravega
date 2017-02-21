/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
