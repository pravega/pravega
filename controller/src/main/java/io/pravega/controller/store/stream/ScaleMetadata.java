/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import lombok.Data;

import java.util.List;

@Data
public class ScaleMetadata {
    /**
     * Time of scale operation.
     */
    private final long timestamp;
    /**
     * Active Segments after the scale operation.
     */
    private final List<Segment> segments;
    /**
     * Number of splits performed as part of scale operation.
     */
    private final long splits;
    /**
     * Number of merges performed as part of scale operation.
     */
    private final long merges;
}
