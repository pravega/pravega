/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.common.Exceptions;
import lombok.Data;

/**
 * Owner of a lock is identified by the owning host and a tag that may represent
 * an identifier for lock attempt.
 */
@Data
public class LockOwner {
    private final String host;
    private final String tag;

    public LockOwner(String host, String tag) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkNotNullOrEmpty(tag, "tag");

        this.host = host;
        this.tag = tag;
    }
}
