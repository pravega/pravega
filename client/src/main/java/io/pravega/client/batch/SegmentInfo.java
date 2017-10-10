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

import io.pravega.client.segment.impl.Segment;
import lombok.Data;

@Data
public class SegmentInfo {

    private final Segment segment;
    private final int epoch;
    private final long length;
    private final long creationTime;
    private final boolean isSealed;
    private final Long endTime;

}
