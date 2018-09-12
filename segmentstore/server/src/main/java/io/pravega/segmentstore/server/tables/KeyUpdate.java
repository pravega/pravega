/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.HashedArray;
import lombok.Getter;

/**
 * Represents an update to a particular Key.
 */
@Getter
class KeyUpdate extends KeyInfo {
    /**
     * If true, indicates the Key has been deleted (as opposed from being updated).
     */
    private final boolean deleted;

    /**
     * Creates a new instance of the KeyUpdate class.
     *
     * @param key     A {@link HashedArray} representing the Key that is updated.
     * @param offset  The offset in the Segment where the update is serialized.
     * @param deleted True if the Key has been deleted via this update, false otherwise.
     */
    KeyUpdate(HashedArray key, long offset, boolean deleted) {
        super(key, offset);
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return (this.deleted ? "[DELETED] " : "") + super.toString();
    }
}
