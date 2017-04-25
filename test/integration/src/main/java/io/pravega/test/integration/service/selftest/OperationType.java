/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.service.selftest;

import io.pravega.common.Exceptions;

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
