/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.checkpoint;

import com.emc.pravega.controller.server.ControllerServerException;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Controller store exception.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CheckpointStoreException extends ControllerServerException {

    public enum Type {
        Unknown,
        NodeExists,
        NoNode,
        NodeNotEmpty,
        Active,
        Sealed,
    }

    private final Type type;

    public CheckpointStoreException(Throwable t) {
        super(t);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, Throwable t) {
        super(t);
        this.type = type;
    }

    public CheckpointStoreException(String message) {
        super(message);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, String message) {
        super(message);
        this.type = type;
    }

    public CheckpointStoreException(String message, Throwable t) {
        super(message, t);
        this.type = Type.Unknown;
    }

    public CheckpointStoreException(Type type, String message, Throwable t) {
        super(message, t);
        this.type = Type.Unknown;
    }
}
