/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.shared.Exceptions;

/**
 * Defines the base for an Operation Type.
 */
abstract class OperationType {
    private final String name;

    OperationType(String name) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
