/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

/**
 * Intentional exception to be thrown inside unit tests.
 */
public class IntentionalException extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public IntentionalException() {
        this("intentional");
    }

    public IntentionalException(String message) {
        super(message);
    }
}
