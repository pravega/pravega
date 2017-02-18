/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.stats;

public interface SegmentStatsRecorder {

    void createSegment(String streamSegmentName, byte type, int targetRate);

    void sealSegment(String streamSegmentName);

    void policyUpdate(String segmentStreamName, byte type, int targetRate);

    void record(String streamSegmentName, long dataLength, int numOfEvents);

    void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime);
}
