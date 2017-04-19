/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;
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

    public LockData(LockOwner lockOwner, TaskData taskData) {
        this(lockOwner, taskData.serialize());
    }

    public LockData(LockOwner lockOwner, byte[] taskData) {
        this.hostId = lockOwner.getHost();
        this.tag = lockOwner.getTag();
        this.taskData = taskData;
    }

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public boolean isOwnedBy(final String owner, final String ownerTag) {
        return hostId != null
                && hostId.equals(owner)
                && tag != null
                && tag.equals(ownerTag);
    }

    public boolean isOwnedBy(final LockOwner lockOwner) {
        return hostId != null
                && hostId.equals(lockOwner.getHost())
                && tag != null
                && tag.equals(lockOwner.getTag());
    }

    public static LockData deserialize(final byte[] bytes) {
        return (LockData) SerializationUtils.deserialize(bytes);
    }
}
