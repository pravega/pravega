/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

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
