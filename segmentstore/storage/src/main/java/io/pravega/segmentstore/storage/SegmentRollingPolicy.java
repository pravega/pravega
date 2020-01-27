/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * A generic rolling policy that can be applied to any Storage unit.
 */
public final class SegmentRollingPolicy {
    public static final SegmentRollingPolicy NO_ROLLING = new SegmentRollingPolicy(Long.MAX_VALUE);

    /**
     * Maximum length, as allowed by this Rolling Policy.
     */
    @Getter
    private final long maxLength;

    /**
     * Creates a new instance of the Rolling Policy class.
     *
     * @param maxLength The maximum length as allowed by this Rolling Policy.
     */
    public SegmentRollingPolicy(long maxLength) {
        Preconditions.checkArgument(maxLength > 0, "maxLength must be a positive number.");
        this.maxLength = maxLength;
    }

    @Override
    public String toString() {
        return String.format("MaxLength = %d", this.maxLength);
    }
}
