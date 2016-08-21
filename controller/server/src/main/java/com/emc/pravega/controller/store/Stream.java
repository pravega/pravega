package com.emc.pravega.controller.store;

import lombok.Data;
import lombok.ToString;

/**
 * Stream properties
 */
@Data
@ToString(includeFieldNames=true)
public class Stream {
    private String name;
    private SegmentStore segmentStore;

    Stream(String name, SegmentStore segmentStore) {
        this.name = name;
        this.segmentStore = segmentStore;
    }
}
