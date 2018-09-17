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
    public static final String SEGMENT_CREATE_LATENCY = "segmentstore.segment_create_latency_ms"; // Timer
    public static final String SEGMENT_READ_LATENCY = "segmentstore.segment_read_latency_ms";     // Timer
    public static final String SEGMENT_WRITE_LATENCY = "segmentstore.segment_write_latency_ms";   // Timer
    public static final String SEGMENT_READ_BYTES = "segmentstore.segment_read_bytes";            // Dynamic Counter
    public static final String SEGMENT_WRITE_BYTES = "segmentstore.segment_write_bytes";          // Dynamic Counter
    public static final String SEGMENT_WRITE_EVENTS = "segmentstore.segment_write_events";        // Dynamic Counter

    //storage stats
    public static final String STORAGE_READ_LATENCY = "segmentstore.storage.read_latency_ms";   // Timer
    public static final String STORAGE_WRITE_LATENCY = "segmentstore.storage.write_latency_ms"; // Timer
    public static final String STORAGE_READ_BYTES = "segmentstore.storage.read_bytes";          // Counter
    public static final String STORAGE_WRITE_BYTES = "segmentstore.storage.write_bytes";        // Counter

    //Cache (RocksDB) stats
    public static final String CACHE_INSERT_LATENCY = "segmentstore.cache_insert_latency";
    public static final String CACHE_GET_LATENCY = "segment.store.cache_get_latency";

    //DurableDataLog (Tier1) stats
    public static final String BK_TOTAL_WRITE_LATENCY = "segmentstore.bookkeeper.total_write_latency"; // Including Queue
    public static final String BK_WRITE_LATENCY = "segmentstore.bookkeeper.write_latency";  // Exclusively the write to BK.
    public static final String BK_WRITE_BYTES = "segmentstore.bookkeeper.write_bytes";
    public static final String BK_WRITE_QUEUE_SIZE = "segmentstore.bookkeeper.write_queue_size";
    public static final String BK_WRITE_QUEUE_FILL_RATE = "segmentstore.bookkeeper.write_queue_fill";
    public static final String BK_LEDGER_COUNT = "segmentstore.bookkeeper.bookkeeper_ledger_count";

    //Container-specific metrics
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
    public static final String PROCESS_OPERATIONS_LATENCY = "segmentstore.process.operations_latency";
    public static final String PROCESS_OPERATIONS_BATCH_SIZE = "segmentstore.process.operations_batch_size";
    public static final String OPERATION_QUEUE_SIZE = "segmentstore.operation.queue_size";
    public static final String OPERATION_PROCESSOR_IN_FLIGHT = "segmentstore.operation.processor_in_flight";
    public static final String OPERATION_QUEUE_WAIT_TIME = "segmentstore.operation.queue_wait_time";
    public static final String OPERATION_PROCESSOR_DELAY_MILLIS = "segmentstore.operation.processor_delay_ms";
    public static final String OPERATION_COMMIT_LATENCY = "segmentstore.operation.commit_latency_ms";
    public static final String OPERATION_LATENCY = "segmentstore.operation.latency_ms";
    public static final String OPERATION_COMMIT_METADATA_TXN_COUNT = "segmentstore.operation.commit_metadata_txn_count";
    public static final String OPERATION_COMMIT_MEMORY_LATENCY = "segmentstore.operation.commit_memory_latency_ms";
    public static final String OPERATION_LOG_SIZE = "segmentstore.operation.log_size";
    public static final String ACTIVE_SEGMENT_COUNT = "segmentstore.active_segments";

    // General metrics
    public static final String CACHE_TOTAL_SIZE_BYTES = "segmentstore.cache_size_bytes";
    public static final String CACHE_GENERATION_SPREAD = "segmentstore.cache_gen";
    public static final String THREAD_POOL_QUEUE_SIZE = "segmentstore.thread_pool_queue_size";
    public static final String THREAD_POOL_ACTIVE_THREADS = "segmentstore.thread_pool_active_threads";

    // Metrics in Controller
    // Stream request counts (Static)
    public static final String CREATE_STREAM = "controller.stream_created";    // Histogram
    public static final String SEAL_STREAM = "controller.stream_sealed";       // Histogram
    public static final String DELETE_STREAM = "controller.stream_deleted";    // Histogram

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

    // Stream retention operations (Dynamic)
    public static final String RETENTION_FREQUENCY = "controller.retention_frequency";   // Dynamic Counter
    public static final String TRUNCATED_SIZE = "controller.truncated_size"; // Dynamic Counter

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
