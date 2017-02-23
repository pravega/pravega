/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

    /**
     * Method to receive aggregates over past few minutes for a segment.
     *
     * @param streamSegmentName Segment identifier.
     * @param targetRate        policy target rate.
     * @param rateType          policy type.
     * @param startTime         time when segment was created.
     * @param twoMinuteRate     two minute rate.
     * @param fiveMinuteRate    five minute rate.
     * @param tenMinuteRate     ten minute rate.
     * @param twentyMinuteRate  twenty minute rate.
     */
    void process(String streamSegmentName, long targetRate, byte rateType, long startTime, double twoMinuteRate,
                 double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate);

    /**
     * Notify monitor about a state change of a segment.
     *
     * @param streamSegmentName Segment that was either created or sealed
     * @param type              type of change event.
     */
    void notify(String streamSegmentName, NotificationType type);
}
