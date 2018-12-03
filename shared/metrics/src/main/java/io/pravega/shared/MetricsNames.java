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
 * - controller.hosts: metrics related to Pravega servers in the cluster (e.g., number of servers, failures)
 * - controller.container: metrics related to container lifecycle (e.g., failovers)
 *
 * We have two types of metrics:
 * - Global metric: Values are directly associated to the metric name that appears in this file. They are convenient if
 *   we want to report metric values that apply to the whole Pravega cluster (e.g., number of bytes written, operations).
 *   For instance, STORAGE_READ_BYTES can be classified as a global metric.
 *
 * - Object-based metric: Sometimes, we may want to report metrics based on specific objects, such as Streams or Segments.
 *   This kind of metrics use as a base name the metric name in this file and are "dynamically" created based on the
 *   objects to be measured. For instance, in CONTAINER_APPEND_COUNT we actually report multiple metrics, one per each
 *   containerId measured: segmentstore.container.append_count.$containerId.
 *
 * There are cases in which we may want both a global and object-based versions for the same metric. For example,
 * regarding SEGMENT_READ_BYTES we publish the a simple counter (segmentstore.segment.read_bytes_global) to easily get
 * the global number of bytes read, as well as the per-segment version of it (segmentstore.segment.read_bytes.$segmentName)
 * to report in a finer granularity the events read per segment. As can be noted, when a metric is used to simultaneously
 * get both global and object-based variants, we add the "_global" suffix to the global metric name to avoid naming
 * collisions.
 */


public final class MetricsNames {
    // Metrics in Segment Store Service
    // Segment-related stats
    public static final String SEGMENT_CREATE_LATENCY = "segmentstore.segment.create_latency_ms"; // Histogram
    public static final String SEGMENT_READ_LATENCY = "segmentstore.segment.read_latency_ms";     // Histogram
    public static final String SEGMENT_WRITE_LATENCY = "segmentstore.segment.write_latency_ms";   // Histogram
    public static final String SEGMENT_READ_BYTES = "segmentstore.segment.read_bytes";            // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_BYTES = "segmentstore.segment.write_bytes";          // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_EVENTS = "segmentstore.segment.write_events";        // Counter and Per-segment Counter

    // Storage stats
    public static final String STORAGE_READ_LATENCY = "segmentstore.storage.read_latency_ms";   // Histogram
    public static final String STORAGE_WRITE_LATENCY = "segmentstore.storage.write_latency_ms"; // Histogram
    public static final String STORAGE_READ_BYTES = "segmentstore.storage.read_bytes";          // Counter
    public static final String STORAGE_WRITE_BYTES = "segmentstore.storage.write_bytes";        // Counter
    public static final String STORAGE_CREATE_COUNT = "segmentstore.storage.create_count";      // Counter

    // Cache (RocksDB) stats
    public static final String CACHE_INSERT_LATENCY = "segmentstore.cache.insert_latency_ms";   // Histogram
    public static final String CACHE_WRITE_BYTES = "segmentstore.cache.write_bytes";            // Counter
    public static final String CACHE_GET_LATENCY = "segmentstore.cache.get_latency_ms";         // Histogram
    public static final String CACHE_READ_BYTES = "segmentstore.cache.read_bytes";              // Counter
    public static final String CACHE_TOTAL_SIZE_BYTES = "segmentstore.cache.size_bytes";        // Gauge
    public static final String CACHE_GENERATION_SPREAD = "segmentstore.cache.gen";              // Histogram

    // DurableDataLog (Tier1) stats
    public static final String BK_TOTAL_WRITE_LATENCY = "segmentstore.bookkeeper.total_write_latency_ms";   // Including Queue. Per-container Histogram.
    public static final String BK_WRITE_LATENCY = "segmentstore.bookkeeper.write_latency_ms";               // Exclusively the write to BK. Per-container Histogram.
    public static final String BK_WRITE_BYTES = "segmentstore.bookkeeper.write_bytes";                      // Counter
    public static final String BK_WRITE_QUEUE_SIZE = "segmentstore.bookkeeper.write_queue_size";            // Per-container Histogram
    public static final String BK_WRITE_QUEUE_FILL_RATE = "segmentstore.bookkeeper.write_queue_fill";       // Per-container Histogram
    public static final String BK_LEDGER_COUNT = "segmentstore.bookkeeper.bookkeeper_ledger_count";         // Per-container Gauge

    // Segment container metrics
    public static final String CONTAINER_APPEND_COUNT = "segmentstore.container.append_count";                          // Per-container Event Counter
    public static final String CONTAINER_APPEND_OFFSET_COUNT = "segmentstore.container.append_offset_count";            // Per-container Event Counter
    public static final String CONTAINER_UPDATE_ATTRIBUTES_COUNT = "segmentstore.container.update_attributes_count";    // Per-container Event Counter
    public static final String CONTAINER_GET_ATTRIBUTES_COUNT = "segmentstore.container.get_attributes_count";          // Per-container Event Counter
    public static final String CONTAINER_READ_COUNT = "segmentstore.container.read_count";                              // Per-container Event Counter
    public static final String CONTAINER_GET_INFO_COUNT = "segmentstore.container.get_info_count";                      // Per-container Event Counter
    public static final String CONTAINER_CREATE_SEGMENT_COUNT = "segmentstore.container.create_segment_count";          // Per-container Event Counter
    public static final String CONTAINER_DELETE_SEGMENT_COUNT = "segmentstore.container.delete_segment_count";          // Per-container Event Counter
    public static final String CONTAINER_MERGE_SEGMENT_COUNT = "segmentstore.container.merge_segment_count";            // Per-container Event Counter
    public static final String CONTAINER_SEAL_COUNT = "segmentstore.container.seal_count";                              // Per-container Event Counter
    public static final String CONTAINER_TRUNCATE_COUNT = "segmentstore.container.truncate_count";                      // Per-container Event Counter

    // Operation processor metrics
    public static final String PROCESS_OPERATIONS_LATENCY = "segmentstore.container.process_operations.latency_ms";                 // Per-container Histogram
    public static final String PROCESS_OPERATIONS_BATCH_SIZE = "segmentstore.container.process_operations.batch_size";              // Per-container Histogram
    public static final String OPERATION_QUEUE_SIZE = "segmentstore.container.operation_queue.size";                                // Per-container Histogram
    public static final String OPERATION_PROCESSOR_IN_FLIGHT = "segmentstore.container.operation_processor.in_flight";              // Per-container Histogram
    public static final String OPERATION_QUEUE_WAIT_TIME = "segmentstore.container.operation_queue.wait_time";                      // Per-container Histogram
    public static final String OPERATION_PROCESSOR_DELAY_MILLIS = "segmentstore.container.operation_processor.delay_ms";            // Per-container Histogram
    public static final String OPERATION_COMMIT_LATENCY = "segmentstore.container.operation_commit.latency_ms";                     // Per-container Histogram
    public static final String OPERATION_LATENCY = "segmentstore.container.operation.latency_ms";                                   // Per-container Histogram
    public static final String OPERATION_COMMIT_METADATA_TXN_COUNT = "segmentstore.container.operation_commit.metadata_txn_count";  // Per-container Histogram
    public static final String OPERATION_COMMIT_MEMORY_LATENCY = "segmentstore.container.operation_commit.memory_latency_ms";       // Per-container Histogram
    public static final String OPERATION_LOG_SIZE = "segmentstore.container.operation.log_size";                                    // Per-container Counter

    // Segment container metadata
    public static final String ACTIVE_SEGMENT_COUNT = "segmentstore.active_segments";   // Per-container Gauge

    // Thread pool metrics
    public static final String THREAD_POOL_QUEUE_SIZE = "segmentstore.thread_pool.queue_size";          // Histogram
    public static final String THREAD_POOL_ACTIVE_THREADS = "segmentstore.thread_pool.active_threads";  // Histogram

    // Metrics in Controller
    // Stream request counts
    public static final String CREATE_STREAM = "controller.stream.created";                         // Counter
    public static final String CREATE_STREAM_LATENCY = "controller.stream.created_latency_ms";      // Histogram
    public static final String CREATE_STREAM_FAILED = "controller.stream.create_failed";            // Counter and Per-stream Counter
    public static final String SEAL_STREAM = "controller.stream.sealed";                            // Counter
    public static final String SEAL_STREAM_LATENCY = "controller.stream.sealed_latency_ms";         // Histogram
    public static final String SEAL_STREAM_FAILED = "controller.stream.seal_failed";                // Counter and Per-stream Counter
    public static final String DELETE_STREAM = "controller.stream.deleted";                         // Counter
    public static final String DELETE_STREAM_LATENCY = "controller.stream.deleted_latency_ms";      // Histogram
    public static final String DELETE_STREAM_FAILED = "controller.stream.delete_failed";            // Counter and Per-stream Counter
    public static final String UPDATE_STREAM = "controller.stream.updated";                         // Counter and Per-stream Counter
    public static final String UPDATE_STREAM_LATENCY = "controller.stream.updated_latency_ms";      // Histogram
    public static final String UPDATE_STREAM_FAILED = "controller.stream.update_failed";            // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM = "controller.stream.truncated";                     // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM_LATENCY = "controller.stream.truncated_latency_ms";  // Histogram
    public static final String TRUNCATE_STREAM_FAILED = "controller.stream.truncate_failed";        // Counter and Per-stream Counter

    // Transaction request Operations
    public static final String CREATE_TRANSACTION = "controller.transactions.created";                      // Counter and Per-stream Counter
    public static final String CREATE_TRANSACTION_LATENCY = "controller.transactions.created_latency_ms";   // Histogram
    public static final String CREATE_TRANSACTION_FAILED = "controller.transactions.create_failed";         // Counter and Per-stream Counter
    public static final String COMMIT_TRANSACTION = "controller.transactions.committed";                    // Counter and Per-stream Counter
    public static final String COMMIT_TRANSACTION_LATENCY = "controller.transactions.committed_latency_ms"; // Histogram
    public static final String COMMIT_TRANSACTION_FAILED = "controller.transactions.commit_failed";         // Counter, Per-stream Counter, Per-transaction Counter
    public static final String ABORT_TRANSACTION = "controller.transactions.aborted";                       // Counter and Per-stream Counter
    public static final String ABORT_TRANSACTION_LATENCY = "controller.transactions.aborted_latency_ms";    // Histogram
    public static final String ABORT_TRANSACTION_FAILED = "controller.transactions.abort_failed";           // Counter, Per-stream Counter, Per-transaction Counter
    public static final String OPEN_TRANSACTIONS = "controller.transactions.opened";                        // Per-stream Gauge
    public static final String TIMEDOUT_TRANSACTIONS = "controller.transactions.timedout";                  // Per-stream Counter

    // Host metrics
    public static final String SEGMENT_STORE_HOST_NUMBER = "controller.hosts.count";                    // Gauge
    public static final String SEGMENT_STORE_HOST_FAILURES = "controller.hosts.failures";               // Counter and Per-host counter
    public static final String SEGMENT_STORE_HOST_CONTAINER_COUNT = "controller.hosts.container_count"; // Per-host Gauge

    // Container lifecycle metrics
    public static final String CONTAINER_FAILOVERS = "controller.container.failovers";    // Counter and Per-container Counter

    // Stream segment counts
    public static final String SEGMENTS_COUNT = "controller.segments.count";    // Per-stream Gauge
    public static final String SEGMENTS_SPLITS = "controller.segment.splits";   // Per-stream Counter
    public static final String SEGMENTS_MERGES = "controller.segment.merges";   // Per-stream Counter

    // Stream retention operations
    public static final String RETENTION_FREQUENCY = "controller.retention.frequency";   // Per-stream Counter
    public static final String TRUNCATED_SIZE = "controller.retention.truncated_size";   // Per-stream Gauge

    private static String escapeSpecialChar(String name) {
        return name.replace('/', '.').replace(':', '.').replace('|', '.').replaceAll("\\s+", "_");
    }

    public static String nameFromStream(String metric, String scope, String stream) {
        String name = metric + "." + scope + "." + stream;
        return escapeSpecialChar(name);
    }

    public static String nameFromTransaction(String metric, String scope, String stream, String txnId) {
        String name = metric + "." + scope + "." + stream + "." + txnId;
        return escapeSpecialChar(name);
    }

    public static String nameFromSegment(String metric, String segmentName) {
        String name = metric + "." + segmentName;
        return escapeSpecialChar(name);
    }

    public static String nameFromHost(String metric, String hostId) {
        String name = metric + "." + hostId;
        return escapeSpecialChar(name);
    }

    public static String nameFromContainer(String metric, int containerId) {
        String name = metric + "." + containerId;
        return escapeSpecialChar(name);
    }

    public static String globalMetricName(String stringName) {
        return stringName + "_global";
    }
}
