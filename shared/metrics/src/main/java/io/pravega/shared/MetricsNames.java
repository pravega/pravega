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



/*
 * Class that defines all names used for metrics that Pravega exports. All metrics have a prefix that indicates
 * the component that is exporting it. We use a second level to indicate the sub-component (e.g., storage) or
 * abstraction (e.g., transactions) the metric is related to.
 *
 * The current prefix being used are:
 * - segmentstore.segment: metrics for individual segments
 * - segmentstore.storage: metrics related to our long-term storage (Tier 2)
 * - segmentstore.bookkeeper: metrics related to bookkeeper (Tier 1)
 * - segmentstore.container: metrics for segment containers
 * - segmentstore.cache: cache-related metrics (RocksDB)
 * - controller.stream: metrics for operations on streams (e.g., number of streams created)
 * - controller.segments: metrics about segments, per stream (e.g., count, splits, merges)
 * - controller.transactions: metrics related to transactions (e.g., created, committed, aborted)
 * - controller.retention: metrics related to data retention, per stream (e.g., frequency, size of truncated data)
 */


public final class MetricsNames {
    // Metrics in Segment Store Service
    // Segment-related stats
    public static final String SEGMENT_CREATE_LATENCY = "segmentstore.segment.create_latency_ms"; // Timer
    public static final String SEGMENT_READ_LATENCY = "segmentstore.segment.read_latency_ms";     // Timer
    public static final String SEGMENT_WRITE_LATENCY = "segmentstore.segment.write_latency_ms";   // Timer
    public static final String SEGMENT_READ_BYTES = "segmentstore.segment.read_bytes";            // Dynamic Counter
    public static final String SEGMENT_WRITE_BYTES = "segmentstore.segment.write_bytes";          // Dynamic Counter
    public static final String SEGMENT_WRITE_EVENTS = "segmentstore.segment.write_events";        // Dynamic Counter

    // Storage stats
    public static final String STORAGE_READ_LATENCY = "segmentstore.storage.read_latency_ms";   // Timer
    public static final String STORAGE_WRITE_LATENCY = "segmentstore.storage.write_latency_ms"; // Timer
    public static final String STORAGE_READ_BYTES = "segmentstore.storage.read_bytes";          // Counter
    public static final String STORAGE_WRITE_BYTES = "segmentstore.storage.write_bytes";        // Counter

    // Cache (RocksDB) stats
    public static final String CACHE_INSERT_LATENCY = "segmentstore.cache.insert_latency_ms";
    public static final String CACHE_GET_LATENCY = "segment.store.cache.get_latency_ms";
    public static final String CACHE_TOTAL_SIZE_BYTES = "segmentstore.cache.size_bytes";
    public static final String CACHE_GENERATION_SPREAD = "segmentstore.cache.gen";

    // DurableDataLog (Tier1) stats
    public static final String BK_TOTAL_WRITE_LATENCY = "segmentstore.bookkeeper.total_write_latency_ms"; // Including Queue
    public static final String BK_WRITE_LATENCY = "segmentstore.bookkeeper.write_latency_ms";  // Exclusively the write to BK.
    public static final String BK_WRITE_BYTES = "segmentstore.bookkeeper.write_bytes";
    public static final String BK_WRITE_QUEUE_SIZE = "segmentstore.bookkeeper.write_queue_size";
    public static final String BK_WRITE_QUEUE_FILL_RATE = "segmentstore.bookkeeper.write_queue_fill";
    public static final String BK_LEDGER_COUNT = "segmentstore.bookkeeper.bookkeeper_ledger_count";

    // Segment container metrics
    public static final String CONTAINER_APPEND_COUNT = "segmentstore.container.append_count";
    public static final String CONTAINER_APPEND_OFFSET_COUNT = "segmentstore.container.append_offset_count";
    public static final String CONTAINER_UPDATE_ATTRIBUTES_COUNT = "segmentstore.container.update_attributes_count";
    public static final String CONTAINER_GET_ATTRIBUTES_COUNT = "segmentstore.container.get_attributes_count";
    public static final String CONTAINER_READ_COUNT = "segmentstore.container.read_count";
    public static final String CONTAINER_GET_INFO_COUNT = "segmentstore.container.get_info_count";
    public static final String CONTAINER_CREATE_SEGMENT_COUNT = "segmentstore.container.create_segment_count";
    public static final String CONTAINER_DELETE_SEGMENT_COUNT = "segmentstore.container.delete_segment_count";
    public static final String CONTAINER_MERGE_SEGMENT_COUNT = "segmentstore.container.merge_segment_count";
    public static final String CONTAINER_SEAL_COUNT = "segmentstore.container.seal_count";
    public static final String CONTAINER_TRUNCATE_COUNT = "segmentstore.container.truncate_count";

    // Operation processor metrics
    public static final String PROCESS_OPERATIONS_LATENCY = "segmentstore.container.process_operations.latency_ms";
    public static final String PROCESS_OPERATIONS_BATCH_SIZE = "segmentstore.container.process_operations.batch_size";
    public static final String OPERATION_QUEUE_SIZE = "segmentstore.container.operation_queue.size";
    public static final String OPERATION_PROCESSOR_IN_FLIGHT = "segmentstore.container.operation_processor.in_flight";
    public static final String OPERATION_QUEUE_WAIT_TIME = "segmentstore.container.operation_queue.wait_time";
    public static final String OPERATION_PROCESSOR_DELAY_MILLIS = "segmentstore.container.operation_processor.delay_ms";
    public static final String OPERATION_COMMIT_LATENCY = "segmentstore.container.operation_commit.latency_ms";
    public static final String OPERATION_LATENCY = "segmentstore.container.operation.latency_ms";
    public static final String OPERATION_COMMIT_METADATA_TXN_COUNT = "segmentstore.container.operation_commit.metadata_txn_count";
    public static final String OPERATION_COMMIT_MEMORY_LATENCY = "segmentstore.container.operation_commit.memory_latency_ms";
    public static final String OPERATION_LOG_SIZE = "segmentstore.container.operation.log_size";

    // Segment container metadata
    public static final String ACTIVE_SEGMENT_COUNT = "segmentstore.active_segments";

    // Thread pool metrics
    public static final String THREAD_POOL_QUEUE_SIZE = "segmentstore.thread_pool.queue_size";
    public static final String THREAD_POOL_ACTIVE_THREADS = "segmentstore.thread_pool.active_threads";

    // Metrics in Controller
    // Stream request counts (Static)
    public static final String CREATE_STREAM = "controller.stream.created";    // Histogram
    public static final String SEAL_STREAM = "controller.stream.sealed";       // Histogram
    public static final String DELETE_STREAM = "controller.stream.deleted";    // Histogram

    // Transaction request Operations (Dynamic)
    public static final String CREATE_TRANSACTION = "controller.transactions.created";   // Dynamic Counter
    public static final String COMMIT_TRANSACTION = "controller.transactions.committed"; // Dynamic Counter
    public static final String ABORT_TRANSACTION = "controller.transactions.aborted";    // Dynamic Counter
    public static final String OPEN_TRANSACTIONS = "controller.transactions.opened";     // Dynamic Gauge
    public static final String TIMEDOUT_TRANSACTIONS = "controller.transactions.timedout";     // Dynamic Counter

    // Stream segment counts (Dynamic)
    public static final String SEGMENTS_COUNT = "controller.segments.count";   // Dynamic Gauge
    public static final String SEGMENTS_SPLITS = "controller.segment.splits"; // Dynamic Counter
    public static final String SEGMENTS_MERGES = "controller.segment.merges"; // Dynamic Counter

    // Stream retention operations (Dynamic)
    public static final String RETENTION_FREQUENCY = "controller.retention.frequency";   // Dynamic Counter
    public static final String TRUNCATED_SIZE = "controller.retention.truncated_size"; // Dynamic Counter

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

    public static String nameFromContainer(String metric, int containerId) {
        String name = metric + "." + containerId;
        return escapeSpecialChar(name);
    }
}
