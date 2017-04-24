/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server;

import io.pravega.common.netty.WireCommandType;
import io.pravega.controller.retryable.RetryableException;

/**
 * Wire command failed exception.
 */
public class WireCommandFailedException extends RuntimeException implements RetryableException {

    public enum Reason {
        ConnectionDropped,
        ConnectionFailed,
        UnknownHost,
        PreconditionFailed,
    }

    private final WireCommandType type;
    private final Reason reason;

    public WireCommandFailedException(Throwable cause, WireCommandType type, Reason reason) {
        super(cause);
        this.type = type;
        this.reason = reason;
    }

    public WireCommandFailedException(WireCommandType type, Reason reason) {
        super(String.format("WireCommandFailed with type %s reason %s", type.toString(), reason.toString()));
        this.type = type;
        this.reason = reason;
    }
}
