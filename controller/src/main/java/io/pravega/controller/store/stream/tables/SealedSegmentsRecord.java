/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Data class for storing information about stream's truncation point.
 */
public class SealedSegmentsRecord implements Serializable {
    /**
     * Sealed segments with size at the time of sealing.
     */
    private final Map<Integer, Long> sealedSegmentsSizeMap;

    public SealedSegmentsRecord(Map<Integer, Long> sealedSegmentsSizeMap) {
        this.sealedSegmentsSizeMap = Collections.unmodifiableMap(sealedSegmentsSizeMap);
    }

    public Map<Integer, Long> getSealedSegmentsSizeMap() {
        return Collections.unmodifiableMap(sealedSegmentsSizeMap);
    }
}
