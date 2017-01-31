package com.emc.pravega.service.contracts;

import lombok.Data;

@Data
public class SegmentInfo {
    final String streamSegmentName;
    final boolean autoScale;
    final long desiredRate;
    final boolean rateInBytes;
}
