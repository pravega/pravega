/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.checkpoint;

import io.pravega.controller.server.ControllerServerException;
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
        Connectivity
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
        this.type = type;
    }
}
