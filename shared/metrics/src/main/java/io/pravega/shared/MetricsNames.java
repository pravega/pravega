/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared;

public final class MetricsNames {
    // Metrics in Segment Store Service
    // host stats
    public static final String SEGMENT_CREATE_LATENCY = "segment_create_latency_ms"; // Timer
    public static final String SEGMENT_READ_LATENCY = "segment_read_latency_ms";     // Timer
    public static final String SEGMENT_WRITE_LATENCY = "segment_write_latency_ms";   // Timer
    public static final String SEGMENT_READ_BYTES = "segment_read_bytes";            // Dynamic Counter
    public static final String SEGMENT_WRITE_BYTES = "segment_write_bytes";          // Dynamic Counter

    //hdfs stats
    public static final String HDFS_READ_LATENCY = "hdfs_read_latency_ms";   // Timer
    public static final String HDFS_WRITE_LATENCY = "hdfs_write_latency_ms"; // Timer
    public static final String HDFS_READ_BYTES = "hdfs_read_bytes";          // Counter
    public static final String HDFS_WRITE_BYTES = "hdfs_write_bytes";        // Counter

    //DurableLog stats
    public static final String DURABLE_DATALOG_WRITE_LATENCY = "durable_datalog_write_latency"; // Timer
    public static final String DURABLE_DATALOG_WRITE_BYTES = "durable_datalog_write_bytes";     // Counter

    // Metrics in Controller
    // Stream request counts (Static)
    public static final String CREATE_STREAM = "stream_created";    // Histogram
    public static final String SEAL_STREAM = "stream_sealed";       // Histogram
    public static final String DELETE_STREAM = "stream_deleted";    // Histogram

    // Transaction request Operations (Dynamic)
    public static final String CREATE_TRANSACTION = "transactions_created";   // Dynamic Counter
    public static final String COMMIT_TRANSACTION = "transactions_committed"; // Dynamic Counter
    public static final String ABORT_TRANSACTION = "transactions_aborted";    // Dynamic Counter
    public static final String OPEN_TRANSACTIONS = "transactions_opened";     // Dynamic Gauge
    public static final String TIMEDOUT_TRANSACTIONS = "transactions_timedout";     // Dynamic Counter

    // Stream segment counts (Dynamic)
    public static final String SEGMENTS_COUNT = "segments_count";   // Dynamic Gauge
    public static final String SEGMENTS_SPLITS = "segment_splits"; // Dynamic Counter
    public static final String SEGMENTS_MERGES = "segment_merges"; // Dynamic Counter

    private static String escapeSpecialChar(String name) {
        return name.replace('/', '.').replace(':', '.').replace('|', '.').replaceAll("\\s+", "_");
    }

    public static String nameFromStream(String metric, String scope, String stream) {
        String name = scope + "." + stream + "." + metric;
        return escapeSpecialChar(name);
    }

    public static String nameFromSegment(String metric, String segmentName) {
        String name = segmentName + "." + metric;
        return escapeSpecialChar(name);
    }
}
