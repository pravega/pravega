package com.emc.nautilus.streaming;

import java.io.Serializable;

import com.emc.nautilus.logclient.SegmentOutputConfiguration;

import lombok.Data;

@Data
public class ProducerConfig implements Serializable {

    private final SegmentOutputConfiguration segmentConfig;

}
