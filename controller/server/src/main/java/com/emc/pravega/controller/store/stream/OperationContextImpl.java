/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import lombok.Getter;

class OperationContextImpl implements OperationContext {

    @Getter
    private final transient Stream stream;

    OperationContextImpl(Stream stream) {
        this.stream = stream;
    }
}
