/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;

import com.emc.pravega.stream.impl.segment.SegmentInputConfiguration;

@Data
@Builder
public class ReaderConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SegmentInputConfiguration segmentConfig;

}
