/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;


@Data
public class StreamCutImpl extends StreamCutInternal {

    private final Stream stream;
    @Getter(value = AccessLevel.PACKAGE)
    private final Map<Segment, Long> positions;
    
}
