/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package io.pravega.framework;

import lombok.extern.slf4j.Slf4j;

/**
 * TestFrameworkException is notify / convey errors (non-recoverable) while interacting with Marathon, Metronome
 * frameworks.
 */
@Slf4j
public class TestFrameworkException extends RuntimeException {
    public enum Type {
        ConnectionFailed,
        RequestFailed,
        LoginFailed,
        InternalError,
    }

    private final Type type;

    public TestFrameworkException(Type type, String reason, Throwable cause) {
        super(reason, cause);
        this.type = type;
        log.error("TestFramework Exception. Type: {}, Details: {}", type, reason, cause);
    }

    public TestFrameworkException(Type type, String reason) {
        super(reason);
        this.type = type;
        log.error("TestFramework Exception. Type: {}, Details: {}", type, reason);
    }
}
