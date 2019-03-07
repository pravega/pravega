/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.util.ArrayView;
import lombok.Data;

/**
 * An update to a Table.
 */
@Data
class TableUpdate implements ProducerUpdate {
    /**
     * If true, this should result in a Removal (as opposed from an update).
     */
    private final boolean removal;

    /**
     * Key to update/remove.
     */
    private final ArrayView key;
    /**
     * Value to associate with the key. Null if {@link #isRemoval()} is true.
     */
    private final ArrayView value;
    /**
     * If non-null, this is a conditional update/removal, and this represents the condition.
     */
    private final Long compareVersion;

    @Override
    public String toString() {
        return String.format("%s Key:%d, Value:%d, Version=%s",
                isRemoval() ? "Remove" : "Update", this.key.getLength(), this.value == null ? 0 : this.value.getLength(),
                this.compareVersion == null ? "(null)" : this.compareVersion.toString());
    }
}
