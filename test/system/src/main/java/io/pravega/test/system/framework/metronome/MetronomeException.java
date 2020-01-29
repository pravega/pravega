/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.metronome;

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
     * @return  status code.
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
