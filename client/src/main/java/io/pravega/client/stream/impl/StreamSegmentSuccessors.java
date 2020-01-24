/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import java.util.Set;
import lombok.Data;

/**
 * Successor segments of a given segment.
 */
@Data
public class StreamSegmentSuccessors {
    private final Set<Segment> segments;
    private final String delegationToken;
}
