/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.testcommon;

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
