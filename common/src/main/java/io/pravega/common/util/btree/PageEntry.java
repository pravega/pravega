/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A Key-Value pair of ByteArraySegments that represent an entry in a B+Tree Page.
 */
@Getter
@RequiredArgsConstructor
public class PageEntry {
    /**
     * The Key.
     */
    @NonNull
    private final ByteArraySegment key;
    /**
     * The Value.
     */
    private final ByteArraySegment value;

    boolean hasValue() {
        return this.value != null;
    }

    @Override
    public String toString() {
        return String.format("KeyLength = %s, ValueLength = %s", this.key.getLength(),
                this.value == null ? "[NO VALUE]" : this.value.getLength());
    }
}
