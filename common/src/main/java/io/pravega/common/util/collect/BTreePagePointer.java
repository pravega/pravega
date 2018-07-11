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

import io.pravega.common.util.ByteArraySegment;
import lombok.Data;

@Data
class BTreePagePointer {
    private final ByteArraySegment key;
    private final long offset;
    private final int length;
}
