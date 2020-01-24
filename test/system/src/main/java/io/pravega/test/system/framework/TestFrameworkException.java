/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import lombok.extern.slf4j.Slf4j;

/**
 * TestFrameworkException is notify / convey errors (non-recoverable) while interacting with Marathon, Metronome
 * frameworks.
 */
@Slf4j
public class TestFrameworkException extends RuntimeException {
    private static final long serialVersionUID = 1L;

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
