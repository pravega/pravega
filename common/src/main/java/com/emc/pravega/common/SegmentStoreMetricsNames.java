/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common;

public final class SegmentStoreMetricsNames {
    // Stream Segment request Operations
    public static final String CREATE_SEGMENT = "CreateSegment";

    // Bytes read by READ_SEGMENT operation, for read throughput
    public static final String SEGMENT_READ_BYTES = "SegmentReadBytes";
    public static final String SEGMENT_READ_LATENCY = "SegmentReadLatencyMillis";

    public static final String SEGMENT_WRITE_BYTES = "SegmentWriteBytes";
    public static final String SEGMENT_WRITE_LATENCY = "SegmentWriteLatencyMillis";

    // Gauge for pending append bytes
    public static final String PENDING_APPEND_BYTES = "PendingAppendBytes";

    //HDFS stats
    public static final String HDFS_READ_LATENCY = "HDFSReadLatencyMillis";
    public static final String HDFS_WRITE_LATENCY = "HDFSWriteLatencyMillis";
    public static final String HDFS_READ_BYTES = "HDFSReadBytes";
    public static final String HDFS_WRITE_BYTES = "HDFSWriteBytes";

    public static String nameFromSegment(String metric, String segmentName) {
        return metric + "." + segmentName.replace('/', '_');
    }
}
