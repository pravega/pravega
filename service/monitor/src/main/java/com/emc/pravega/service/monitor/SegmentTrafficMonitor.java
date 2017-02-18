/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.monitor;

/**
 * Interface for monitors that would monitor traffic for all segments.
 * The process method of this interface is called periodically with aggregates sent over.
 * Implementations of this interface define what needs to be done with these aggregates.
 */
public interface SegmentTrafficMonitor {

    enum NotificationType {
        SegmentCreated,
        SegmentSealed
    }

    void process(String streamSegmentName, long targetRate, byte rateType, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate);

    void notify(String streamSegmentName, NotificationType type);
}
