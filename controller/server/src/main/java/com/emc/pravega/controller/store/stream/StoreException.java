/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

    private Type type;
    private String path;

    /**
     * Construct a StoreException.
     *
     * @param type Type of Exception.
     */
    public StoreException(final Type type) {
        this.type = type;
    }

    /**
     * Constructs a StoreException.
     *
     * @param type Type of Exception.
     * @param path The ZooKeeper path being operated on.
     */
    public StoreException(final Type type, final String path) {
        this.type = type;
        this.path = path;
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
        switch (type) {
            case NODE_EXISTS:
                return new NodeExistsException();
            case NODE_NOT_FOUND:
                return new NodeNotFoundException();
            case NODE_NOT_EMPTY:
                return new NodeNotEmptyException();
            case UNKNOWN:
                return new UnknownException();
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
    }

    /**
     * Exception type when node exists, and duplicate node is created.
     */
    public static class NodeExistsException extends StoreException {
        public NodeExistsException() {
            super(Type.NODE_EXISTS);
        }
    }

    /**
     * Exception type when node does not exist and is operated on.
     */
    public static class NodeNotFoundException extends StoreException {
        public NodeNotFoundException() {
            super(Type.NODE_NOT_FOUND);
        }
    }

    /**
     * Exception type when deleting a non empty node.
     */
    public static class NodeNotEmptyException extends StoreException {
        public NodeNotEmptyException() {
            super(Type.NODE_NOT_EMPTY);
        }
    }

    /**
     * Exception type when the cause is not known.
     */
    public static class UnknownException extends StoreException {
        public UnknownException() {
            super(Type.UNKNOWN);
        }
    }

}
