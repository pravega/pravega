package com.emc.pravega.service.server.host.stats;

import com.emc.pravega.service.contracts.SegmentInfo;

class SegmentAggregates {
    final String streamSegmentName;
    final boolean autoScale;
    final long targetRate;
    final byte rateType;

    long twoMinuteRate;
    long fiveMinuteRate;
    long tenMinuteRate;

    long twentyMinuteRate;

    long lastUpdateTime;
    long lastReportedTime;

    SegmentAggregates(SegmentInfo info) {
        streamSegmentName = info.getStreamSegmentName();
        autoScale = info.isAutoScale();
        targetRate = info.getTargetRate();
        rateType = info.getRateType();
    }

    SegmentAggregates(String streamSegmentName, boolean autoScale, long targetRate, byte rateType, long twoMinuteRate, long fiveMinuteRate, long tenMinuteRate, long twentyMinuteRate, long lastUpdateTime, long lastReportedTime) {
        this.streamSegmentName = streamSegmentName;
        this.autoScale = autoScale;
        this.targetRate = targetRate;
        this.rateType = rateType;
        this.twoMinuteRate = twoMinuteRate;
        this.fiveMinuteRate = fiveMinuteRate;
        this.tenMinuteRate = tenMinuteRate;
        this.twentyMinuteRate = twentyMinuteRate;
        this.lastUpdateTime = lastUpdateTime;
        this.lastReportedTime = lastReportedTime;
    }
}
