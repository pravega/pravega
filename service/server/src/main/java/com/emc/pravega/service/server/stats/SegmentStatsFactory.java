/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.stats;

import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.Data;

import java.util.List;

@Data
public class SegmentStatsFactory {
    private final List<SegmentTrafficMonitor> monitors;

    public SegmentStatsRecorder createSegmentStatsRecorder(CacheFactory cacheFactory) {
        return new SegmentStatsRecorderImpl(monitors, cacheFactory, new JavaSerializer<>());
    }
}
