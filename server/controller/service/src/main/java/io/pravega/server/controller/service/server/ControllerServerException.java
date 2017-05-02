/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server;

/**
 * Base class for exceptions thrown from controller server code base.
 */
public class ControllerServerException extends Exception {

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
