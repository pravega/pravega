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
import lombok.Getter;
import lombok.NonNull;

/**
 * An Entry in a Table Segment, made up of a Key and a Value, with optional Version.
 */
@Getter
public class TableEntry extends TableKey {
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
    public TableEntry(ArrayView key, @NonNull ArrayView value) {
        super(key);
        this.value = value;
    }

    /**
     * Creates a new instance of the TableEntry class with a specified version.
     *
     * @param key     The Key.
     * @param value   The Value.
     * @param version The desired Version. Must be non-negative.
     */
    public TableEntry(ArrayView key, @NonNull ArrayView value, long version) {
        super(key, version);
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", super.toString(), this.value);
    }

}
