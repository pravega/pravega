/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;

/**
 * Version of a Key in a Table.
 */
public interface KeyVersion {
    /**
     * {@link KeyVersion} that indicates no specific version is desired. Using this will result in an unconditional
     * update or removal being performed. See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    KeyVersion NO_VERSION = new KeyVersionImpl(null, TableSegmentKeyVersion.NO_VERSION);
    /**
     * {@link KeyVersion} that indicates the {@link TableKey} must not exist. Using this will result in an conditional
     * update or removal being performed, conditioned on the {@link TableKey} not existing at the time of the operation.
     * See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    KeyVersion NOT_EXISTS = new KeyVersionImpl(null, TableSegmentKeyVersion.NOT_EXISTS);

    /**
     * Returns the actual instance.
     * This method prevents other classes from implementing this interface.
     *
     * @return Implementation of the {@link KeyVersion} interface.
     */
    KeyVersionImpl asImpl();

    /**
     * Serializes the {@link KeyVersion} to a human readable string.
     *
     * @return A string representation of the {@link KeyVersion}.
     */
    @Override
    String toString();

    /**
     * Deserializes the {@link KeyVersion} from its serialized form obtained from calling {@link #toString()}.
     *
     * @param str A serialized {@link KeyVersion}.
     * @return The {@link KeyVersion} object.
     */
    static KeyVersion fromString(String str) {
        return KeyVersionImpl.fromString(str);
    }
}
