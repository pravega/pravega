package com.emc.pravega.controller.server;

/**
 * Base class for exceptions thrown from controller server code base.
 */
public class ControllerServerException extends RuntimeException {

    public ControllerServerException(Throwable t) {
        super(t);
    }

    public ControllerServerException(String message) {
        super(message);
    }

    public ControllerServerException(String message, Throwable t) {
        super(message, t);
    }
}
