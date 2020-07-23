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

import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.client.tables.impl.VersionImpl;

/**
 * Version of a Key in a Table.
 */
public interface Version {
    /**
     * {@link Version} that indicates no specific version is desired. Using this will result in an unconditional
     * update or removal being performed. See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    Version NO_VERSION = new VersionImpl(VersionImpl.NO_SEGMENT_ID, TableSegmentKeyVersion.NO_VERSION);
    /**
     * {@link Version} that indicates the {@link TableKey} must not exist. Using this will result in an conditional
     * update or removal being performed, conditioned on the {@link TableKey} not existing at the time of the operation.
     * See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    Version NOT_EXISTS = new VersionImpl(VersionImpl.NO_SEGMENT_ID, TableSegmentKeyVersion.NOT_EXISTS);

    /**
     * Returns the actual instance.
     * This method prevents other classes from implementing this interface.
     *
     * @return Implementation of the {@link Version} interface.
     */
    VersionImpl asImpl();

    /**
     * Serializes the {@link Version} to a human readable string.
     *
     * @return A string representation of the {@link Version}.
     */
    @Override
    String toString();

    /**
     * Deserializes the {@link Version} from its serialized form obtained from calling {@link #toString()}.
     *
     * @param str A serialized {@link Version}.
     * @return The {@link Version} object.
     */
    static Version fromString(String str) {
        return VersionImpl.fromString(str);
    }
}
