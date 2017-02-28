/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.netty.WireCommandType;

/**
 * Wire command failed exception.
 */
public class WireCommandFailedException extends RuntimeException {

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
        this.type = type;
        this.reason = reason;
    }
}
