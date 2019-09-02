/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import java.util.Collection;
import lombok.Getter;

/**
 * Exception that is thrown whenever a Conditional Update to a Table (based on versions) failed.
 */
class ConditionalTableUpdateException extends Exception {
    private static final long serialVersionUID = 1L;
    /**
     * A Collection of Keys that failed conditional update validation.
     */
    @Getter
    private final Collection<TableKey> keys;

    /**
     * Creates a new instance of the ConditionalTableUpdateException class.
     *
     * @param keys A Collection of Keys that failed conditional update validation.
     */
    public ConditionalTableUpdateException(Collection<TableKey> keys) {
        super(String.format("Conditional update failed for %s key(s).", keys.size()));
        this.keys = keys;
    }
}
