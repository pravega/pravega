package com.emc.pravega.service.server.host.stats;

import com.emc.pravega.service.contracts.SegmentInfo;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import lombok.Synchronized;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SegmentStats {
    public static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();

    // segmentInfo -> segment aggregates map

    private static Map<String, SegmentAggregates> aggregatesMap;
    private static List<SegmentTrafficMonitor> monitors;

    @Synchronized
    public static void addMonitor(SegmentTrafficMonitor monitor) {
        monitors.add(monitor);
    }

    public static void record(String segment, long dataLength, int numOfEvents) {
        // do math update aggregates
        SegmentAggregates aggregates = aggregatesMap.get(segment);
        if(System.currentTimeMillis() - aggregates.lastReportedTime > TWO_MINUTES) {
            CompletableFuture.runAsync(() -> monitors.forEach(x -> x.process(segment,
                    aggregates.autoScale, aggregates.targetRate, aggregates.rateType,
                    aggregates.twoMinuteRate, aggregates.fiveMinuteRate, aggregates.tenMinuteRate, aggregates.twentyMinuteRate)));
        }
    }

    @Synchronized
    public static void createSegment(SegmentInfo segment) {
        aggregatesMap.put(segment.getStreamSegmentName(), new SegmentAggregates(segment));
        monitors.forEach(x -> x.notify(segment.getStreamSegmentName(), SegmentTrafficMonitor.NotificationType.SegmentSealed));

    }

    @Synchronized
    public static void sealSegment(String streamSegmentName) {
        if (aggregatesMap.containsKey(streamSegmentName)) {
            SegmentAggregates aggregates = aggregatesMap.get(streamSegmentName);
            SegmentInfo segment = new SegmentInfo(streamSegmentName, aggregates.autoScale, aggregates.targetRate, aggregates.rateType);
            aggregatesMap.remove(streamSegmentName);
            monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentSealed));
        }
    }

    @Synchronized
    public static void policyUpdate(SegmentInfo segment) {
        SegmentAggregates aggregates = aggregatesMap.get(segment.getStreamSegmentName());
        aggregatesMap.put(segment.getStreamSegmentName(),
                new SegmentAggregates(aggregates.streamSegmentName, segment.isAutoScale(),
                        segment.getTargetRate(), segment.getRateType(), aggregates.twoMinuteRate,
                        aggregates.fiveMinuteRate, aggregates.tenMinuteRate, aggregates.twentyMinuteRate,
                        aggregates.lastUpdateTime, aggregates.lastReportedTime));
    }
}
