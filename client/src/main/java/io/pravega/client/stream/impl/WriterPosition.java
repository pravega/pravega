/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.ToString;

@Builder
@ToString
public class WriterPosition {

    private final Map<Segment, Long> segments;

    public Map<Segment, Long> getSegmentsWithOffsets() {
        return Collections.unmodifiableMap(segments);
    }

}