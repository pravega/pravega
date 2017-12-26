/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch;

import com.google.common.annotations.Beta;
import io.pravega.client.segment.impl.Segment;
import lombok.Data;

@Beta
@Data
public class SegmentInfo {

    private final Segment segment;
    private final long startingOffset;
    private final long writeOffset;
    private final boolean isSealed;
    private final long lastModifiedTime;

}
