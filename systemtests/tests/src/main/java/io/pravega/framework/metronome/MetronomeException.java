/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework.metronome;

/**
 * Exceptions while accessing Metronome.
 */
public class MetronomeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private int status;
    private String message;

    public MetronomeException(int status, String message) {
        this.status = status;
        this.message = message;
    }

    /**
     * Gets the HTTP status code of the failure, such as 404.
     */
    public int getStatus() {
        return status;
    }

    @Override
    public String getMessage() {
        return message + " (http status: " + status + ")";
    }

    @Override
    public String toString() {
        return message + " (http status: " + status + ")";
    }
}
