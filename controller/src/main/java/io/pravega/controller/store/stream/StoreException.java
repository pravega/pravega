/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.retryable.RetryableException;
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
        DATA_EXISTS,
        DATA_NOT_FOUND,
        DATA_CONTAINS_ELEMENTS,
        WRITE_CONFLICT,
        ILLEGAL_STATE,
        OPERATION_NOT_ALLOWED,
        CONNECTION_ERROR,
        UNKNOWN
    }

    protected String path;
    private Type type;

    /**
     * Construct a StoreException.
     *
     * @param type  Type of Exception.
     * @param path  The ZooKeeper path being operated on.
     * @param cause The underlying cause for the exception.
     */
    private StoreException(final Type type, final String path, final Throwable cause) {
        super(cause);
        this.type = type;
        this.path = path;
    }

    /**
     * Construct a StoreException.
     *
     * @param type  Type of Exception.
     * @param cause Exception cause.
     */
    public StoreException(final Type type, Throwable cause) {
        super(cause);
        this.type = type;
    }

    /**
     * Constructs a StoreException.
     *
     * @param type  Type of Exception.
     */
    private StoreException(final Type type) {
        this.type = type;
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type Type of Exception.
     * @param path The ZooKeeper path being operated on.
     * @return Instance of StoreException.
     */
    public static StoreException create(Type type, String path) {
        StoreException storeException = create(type);
        storeException.path = path;
        return storeException;
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type Type of Exception.
     * @return Instance of type of StoreException.
     */
    public static StoreException create(final Type type) {
        StoreException exception;
        switch (type) {
            case DATA_EXISTS:
                exception = new DataExistsException();
                break;
            case DATA_NOT_FOUND:
                exception = new DataNotFoundException();
                break;
            case DATA_CONTAINS_ELEMENTS:
                exception = new DataNotEmptyException();
                break;
            case WRITE_CONFLICT:
                exception = new WriteConflictException();
                break;
            case ILLEGAL_STATE:
                exception = new IllegalStateException();
                break;
            case OPERATION_NOT_ALLOWED:
                exception = new OperationNotAllowedException();
                break;
            case CONNECTION_ERROR:
                exception = new StoreConnectionException();
                break;
            case UNKNOWN:
                exception = new UnknownException();
                break;
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
        return exception;
    }

    /**
     * Exception type when node exists, and duplicate node is created.
     */
    public static class DataExistsException extends StoreException {
        public DataExistsException() {
            super(Type.DATA_EXISTS);
        }
    }

    /**
     * Exception type when node does not exist and is operated on.
     */
    public static class DataNotFoundException extends StoreException {
        public DataNotFoundException() {
            super(Type.DATA_NOT_FOUND);
        }
    }

    /**
     * Exception type when deleting a non empty node.
     */
    public static class DataNotEmptyException extends StoreException {
        public DataNotEmptyException() {
            super(Type.DATA_CONTAINS_ELEMENTS);
        }
    }

    /**
     * Exception type when you are attempting to update a stale value.
     */
    public static class WriteConflictException extends StoreException implements RetryableException {
        public WriteConflictException() {
            super(Type.WRITE_CONFLICT);
        }
    }

    /**
     * Exception type when you are attempting a disallowed operation.
     */
    public static class IllegalStateException extends StoreException {
        public IllegalStateException() {
            super(Type.ILLEGAL_STATE);
        }
    }

    /**
     * Exception type when the attempted operation is currently not allowed.
     */
    public static class OperationNotAllowedException extends StoreException implements RetryableException {
        public OperationNotAllowedException() {
            super(Type.OPERATION_NOT_ALLOWED);
        }
    }

    /**
     * Exception type due to failure in connecting to the store.
     */
    public static class StoreConnectionException extends StoreException implements RetryableException {
        public StoreConnectionException() {
            super(Type.CONNECTION_ERROR);
        }
    }

    /**
     * Exception type when the cause is not known.
     */
    public static class UnknownException extends StoreException {
        public UnknownException(final String path, final Throwable cause) {
            super(Type.UNKNOWN, path, cause);
        }

        public UnknownException() {
            super(Type.UNKNOWN);
        }

        public UnknownException(Throwable cause) {
            super(Type.UNKNOWN, cause);
        }
    }
}
