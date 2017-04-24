/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import lombok.Getter;

class OperationContextImpl implements OperationContext {

    @Getter
    private final Stream stream;

    OperationContextImpl(Stream stream) {
        this.stream = stream;
    }
}
