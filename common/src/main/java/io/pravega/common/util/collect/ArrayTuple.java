/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;

@Getter
class ArrayTuple {
    private final ByteArraySegment left;
    private final ByteArraySegment right;

    ArrayTuple(byte[] left, byte[] right) {
        this(new ByteArraySegment(left), new ByteArraySegment(right));
    }

    ArrayTuple(ByteArraySegment left, ByteArraySegment right) {
        this.left = Preconditions.checkNotNull(left, "left");
        this.right = Preconditions.checkNotNull(right, "right");
    }
}
