package com.emc.nautilus.streaming;

import java.util.List;

import lombok.Data;

@Data
public class StreamSegments {
    public final List<SegmentId> segments;
    public final long time;
}