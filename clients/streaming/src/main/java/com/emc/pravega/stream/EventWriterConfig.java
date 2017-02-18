/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;

import lombok.Data;

@Data
public class EventWriterConfig implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final SegmentOutputConfiguration segmentConfig;

}
