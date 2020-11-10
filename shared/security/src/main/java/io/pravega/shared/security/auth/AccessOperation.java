/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.auth;

/**
 * Represents an operation that is authorized by a permission (allow, deny, etc.). Currently, permissions
 * always 'allow' access to specified access operations in Pravega.
 */
public enum AccessOperation {

    /**
     * No operation specified.
     */
    UNSPECIFIED,

    /**
     * Doesn't represent any access operation.
     */
    NONE,

    /**
     * Doesn't represent any specific operation, and allows the caller to make it explicit that any access operation.
     * It is primarily meant for use in tests.
     */
    ANY,

    /**
     * Represents reads.
     */
    READ,

    /**
     * Represents inserts, updates and deletes.
     */
    WRITE,

    /**
     * Represents reads and writes (inserts, updates and deletes).
     */
    READ_WRITE,
}
