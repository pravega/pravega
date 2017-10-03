/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ThreadSafe
@RequiredArgsConstructor
class SubSegment {
    /**
     * The name of the SubSegment.
     */
    @Getter
    private final String name;
    @Getter
    private final long startOffset;
    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean sealed;

    synchronized void markSealed() {
        this.sealed = true;
    }

    synchronized boolean isSealed() {
        return this.sealed;
    }

    synchronized void increaseLength(int delta) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
        this.length += delta;
    }

    synchronized void setLength(long length) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        this.length = length;
    }

    synchronized long getLength() {
        return this.length;
    }

    synchronized long getLastOffset() {
        return this.startOffset + this.length;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%d%s)", this.name, this.length, this.sealed ? ", sealed" : "");
    }
}