/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScaleMetadata {
    /**
     * Time of scale operation.
     */
    private long timestamp;
    /**
     * Active Segments after the scale operation.
     */
    private List<Segment> segments;
    /**
     * Number of splits performed as part of scale operation.
     */
    private long splits;
    /**
     * Number of merges performed as part of scale operation.
     */
    private long merges;
}
