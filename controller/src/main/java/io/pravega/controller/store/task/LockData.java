/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.task;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

/**
 * Lock row
 */
@Data
@EqualsAndHashCode
class LockData implements Serializable {
    private final String hostId;
    private final String tag;
    private final byte[] taskData;

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public boolean isOwnedBy(final String owner, final String ownerTag) {
        return hostId != null
                && hostId.equals(owner)
                && tag != null
                && tag.equals(ownerTag);
    }

    public static LockData deserialize(final byte[] bytes) {
        return (LockData) SerializationUtils.deserialize(bytes);
    }
}
