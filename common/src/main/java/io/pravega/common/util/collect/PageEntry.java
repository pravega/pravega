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
class PageEntry {
    private final ByteArraySegment key;
    private final ByteArraySegment value;

    PageEntry(ByteArraySegment key, ByteArraySegment value) {
        this.key = Preconditions.checkNotNull(key, "key");
        this.value = Preconditions.checkNotNull(value, "value");
    }
}
