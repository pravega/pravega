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

import io.pravega.common.util.ArrayView;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * An Entry in a Table Segment, made up of a Key and a Value, with optional Version.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TableEntry {
    /**
     * The Key.
     */
    private final TableKey key;

    /**
     * The Value (data) of the entry.
     */
    private final ArrayView value;
    /**
     * Creates a new instance of the TableEntry class with no desired version.
     *
     * @param key   The Key.
     * @param value The Value.
     */
    public static TableEntry unversioned(@NonNull ArrayView key, @NonNull ArrayView value) {
        return new TableEntry(TableKey.unversioned(key), value);
    }

    /**
     * Creates a new instance of the TableEntry class that indicates the Key must not previously exist.
     *
     * @param key   The Key.
     * @param value The Value.
     */
    public static TableEntry notExists(@NonNull ArrayView key, @NonNull ArrayView value) {
        return new TableEntry(TableKey.notExists(key), value);
    }

    /**
     * Creates a new instance of the TableEntry class with a specified version.
     *
     * @param key   The Key.
     * @param value The Value.
     * @param version The desired version.
     */
    public static TableEntry versioned(@NonNull ArrayView key, @NonNull ArrayView value, long version) {
        return new TableEntry(TableKey.versioned(key, version), value);
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", this.key, this.value);
    }

}
