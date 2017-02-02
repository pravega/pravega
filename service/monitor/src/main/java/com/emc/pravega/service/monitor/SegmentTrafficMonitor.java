package com.emc.pravega.service.monitor;

public interface SegmentTrafficMonitor {

    enum NotificationType {
        SegmentCreated,
        SegmentSealed
    }

    void process(String streamSegmentName, boolean autoScale, long targetRate, byte rateType, long twoMinuteRate, long fiveMinuteRate, long tenMinuteRate, long twentyMinuteRate);

    void notify(String streamSegmentName, NotificationType type);
}
