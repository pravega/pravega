/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import lombok.Data;

import java.util.AbstractMap;
import java.util.List;

@Data
/**
 * Task subclass to define scaling operations
 * This is serialized and stored in the persistent store
 * and used to resume partially completed scale operation
 */
public final class Scale {
    @Data
    public static class CompleteScale{
        private final List<Integer> sealedSegments;
        private final List<Integer> createdSegments;
    }
}
