/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state;

import java.io.Serializable;

import com.emc.pravega.stream.impl.segment.SegmentInputConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;

import lombok.Data;

/**
 * The configuration for a Consistent replicated state synchronizer.
 */
@Data
public class SynchronizerConfig implements Serializable {

    private final SegmentInputConfiguration inputConfig;
    private final SegmentOutputConfiguration outputConfig;
    
}
