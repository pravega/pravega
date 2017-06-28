/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared;

public final class MetricsNames {
    // Metrics in Segment Store Service
    // Segment-related stats
    public static final String SEGMENT_CREATE_LATENCY = "segment_create_latency_ms"; // Timer
    public static final String SEGMENT_READ_LATENCY = "segment_read_latency_ms";     // Timer
    public static final String SEGMENT_WRITE_LATENCY = "segment_write_latency_ms";   // Timer
    public static final String SEGMENT_READ_BYTES = "segmentstore.segment_read_bytes";            // Dynamic Counter
    public static final String SEGMENT_WRITE_BYTES = "segmentstore.segment_write_bytes";          // Dynamic Counter
    public static final String SEGMENT_WRITE_EVENTS = "segmentstore.segment_write_events";        // Dynamic Counter

    //hdfs stats
    public static final String HDFS_READ_LATENCY = "tier2_read_latency_ms";   // Timer
    public static final String HDFS_WRITE_LATENCY = "tier2_write_latency_ms"; // Timer
    public static final String HDFS_READ_BYTES = "tier2_read_bytes";          // Counter
    public static final String HDFS_WRITE_BYTES = "tier2_write_bytes";        // Counter

    //DurableLog stats
    public static final String TIER1_WRITE_LATENCY = "tier1_datalog_write_latency"; // Timer
    public static final String TIER1_WRITE_BYTES = "tier1_datalog_write_bytes";     // Counter

    // Metrics in Controller
    // Stream request counts (Static)
    public static final String CREATE_STREAM = "stream_created";    // Histogram
    public static final String SEAL_STREAM = "stream_sealed";       // Histogram
    public static final String DELETE_STREAM = "stream_deleted";    // Histogram

    // Transaction request Operations (Dynamic)
    public static final String CREATE_TRANSACTION = "controller.transactions_created";   // Dynamic Counter
    public static final String COMMIT_TRANSACTION = "controller.transactions_committed"; // Dynamic Counter
    public static final String ABORT_TRANSACTION = "controller.transactions_aborted";    // Dynamic Counter
    public static final String OPEN_TRANSACTIONS = "controller.transactions_opened";     // Dynamic Gauge
    public static final String TIMEDOUT_TRANSACTIONS = "controller.transactions_timedout";     // Dynamic Counter

    // Stream segment counts (Dynamic)
    public static final String SEGMENTS_COUNT = "controller.segments_count";   // Dynamic Gauge
    public static final String SEGMENTS_SPLITS = "controller.segment_splits"; // Dynamic Counter
    public static final String SEGMENTS_MERGES = "controller.segment_merges"; // Dynamic Counter

    private static String escapeSpecialChar(String name) {
        return name.replace('/', '.').replace(':', '.').replace('|', '.').replaceAll("\\s+", "_");
    }

    public static String nameFromStream(String metric, String scope, String stream) {
        String name = metric + "." + scope + "." + stream;
        return escapeSpecialChar(name);
    }

    public static String nameFromSegment(String metric, String segmentName) {
        String name = metric + "." + segmentName;
        return escapeSpecialChar(name);
    }
}
