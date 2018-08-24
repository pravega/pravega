/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import lombok.Getter;
import lombok.NonNull;

/**
 * Represents a Key in a Table Segment, with optional version.
 */
@Getter
public class TableKey {
    static final long NO_VERSION = Long.MIN_VALUE;
    /**
     * The Version of the Key. If unversioned, or if no version is desired, this will be a negative value.
     */
    private final long version;

    /**
     * The Key.
     */
    private final ArrayView key;

    /**
     * Creates a new instance of the TableKey class with no desired version.
     *
     * @param key The Key.
     */
    public TableKey(@NonNull ArrayView key) {
        this.key = key;
        this.version = NO_VERSION;
    }

    /**
     * Creates a new instance of the TableKey class with a specified version.
     *
     * @param key     The Key.
     * @param version The desired version. Must be non-negative.
     */
    public TableKey(@NonNull ArrayView key, long version) {
        Preconditions.checkArgument(version >= 0, "Version must be a non-negative number.");
        this.key = key;
        this.version = version;
    }

    /**
     * Gets a value indicating whether this TableKey has a Version defined.
     *
     * @return True if a version is defined, false otherwise. If False, the result of getVersion() is undefined.
     */
    public boolean hasVersion() {
        return this.version >= 0;
    }

    @Override
    public String toString() {
        return String.format("{%s} %s", hasVersion() ? this.version : "*", this.key);
    }

}
