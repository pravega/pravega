package com.emc.logservice.common;

/**
 * Thrown when an object has been closed via AutoCloseable.close().
 */
public class ObjectClosedException extends IllegalStateException {
    public ObjectClosedException(Object object) {
        super(getMessage(object));
    }

    public ObjectClosedException(Object object, Throwable cause) {
        super(getMessage(object), cause);
    }

    private static String getMessage(Object object) {
        if (object == null) {
            return "Object has been closed and cannot be accessed anymore.";
        }

        else {
            return String.format("Object '%s' has been closed and cannot be accessed anymore.", object.getClass().getSimpleName());
        }
    }
}
