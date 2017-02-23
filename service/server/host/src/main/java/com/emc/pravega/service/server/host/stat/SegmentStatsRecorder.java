/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

public interface SegmentStatsRecorder {

    /**
     * Method to notify segment create events.
     *
     * @param streamSegmentName segment.
     * @param type              type of auto scale.
     * @param targetRate        desired rate.
     */
    void createSegment(String streamSegmentName, byte type, int targetRate);

    /**
     * Method to notify segment sealed events.
     *
     * @param streamSegmentName segment.
     * @param streamSegmentName
     */
    void sealSegment(String streamSegmentName);

    /**
     * Method to notify segment policy events.
     *
     * @param streamSegmentName segment.
     * @param type              type of auto scale.
     * @param targetRate        desired rate.
     */
    void policyUpdate(String streamSegmentName, byte type, int targetRate);

    /**
     * Method to record incoming traffic.
     *
     * @param streamSegmentName segment name.
     * @param dataLength        data length.
     * @param numOfEvents       number of events.
     */
    void record(String streamSegmentName, long dataLength, int numOfEvents);

    /**
     * Method to notify merge of transaction.
     *
     * @param streamSegmentName parent segment.
     * @param dataLength        data in transactional segment.
     * @param numOfEvents       events in transactional segment.
     * @param txnCreationTime   transaction creation time.
     */
    void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime);
}
