/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import lombok.RequiredArgsConstructor;

/**
 * Represents a Value that can be versioned.
 */
@RequiredArgsConstructor
class VersionedValue implements Comparable<VersionedValue> {
    /**
     * The version of the Value.
     */
    final long version;

    /**
     * The value itself.
     */
    final long value;

    @Override
    public String toString() {
        return String.format("%d (%d)", this.value, this.version);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VersionedValue) {
            VersionedValue other = (VersionedValue) obj;
            return this.version == other.version && this.value == other.value;
        }

        return false;
    }

    @Override
    public int compareTo(VersionedValue other) {
        int r = Long.compare(this.version, other.version);
        if (r == 0) {
            r = Long.compare(this.value, other.value);
        }
        return r;
    }
}
