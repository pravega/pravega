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
 * Class that defines all names used for metrics that Pravega exports.
 * All metric names are in the format of "Metric prefix + Logger name + Metric name":
 *     Metric prefix is "pravega" by default.
 *     Logger name indicates the component origin of the metric, such as "segmentstore", "controller",
 *         "filesystem", "hdfs", "rocksdb" etc.
 *     Metric names are defined in this class.
 * Note that, counters, gauges and timers belong to dynamic logger, which doesn't have logger name. Hence the first
 *     element of those metric names indicates the component origin.
 *
 * With the resulting complete metric names, the 1st element is the prefix, the 2nd element is the component, and
 * the 3rd elements is "sub-component" or abstraction.
 *
 * Some common combinations of component and sub-components (or abstractions) being used are:
 * - segmentstore.segment: metrics for individual segments
 * - segmentstore.storage: metrics related to our long-term storage (Tier 2)
 * - bookkeeper.segmentstore: metrics related to bookkeeper (Tier 1)
 * - segmentstore.container: metrics for segment containers
 * - segmentstore.thread_pool: metrics for segmentstore thread pool
 * - segmentstore.cache: cache-related metrics (RocksDB)
 * - controller.stream: metrics for operations on streams (e.g., number of streams created)
 * - controller.segments: metrics about segments, per stream (e.g., count, splits, merges)
 * - controller.transactions: metrics related to transactions (e.g., created, committed, aborted)
 * - controller.retention: metrics related to data retention, per stream (e.g., frequency, size of truncated data)
 * - controller.hosts: metrics related to Pravega servers in the cluster (e.g., number of servers, failures)
 * - controller.container: metrics related to container lifecycle (e.g., failovers)
 *
 * So metrics are named differently according to their types:
 * - StatsLogger metrics: The logger name indicates the component origin; so the first element of metric name indicates
 *   the sub-component or abstraction origin.
 *   All histogram metrics are of this type; plus a small number of other types, such as STORAGE_READ_BYTES.
 *
 * - DynamicLogger metrics: There is no logger name for dynamic logger, so the first element of the metric name
 *   is used to indicate the component origin; the 2nd element indicates the sub-component or abstraction origin.
 *   Most counters, gauges and timers are of this type.
 *
 * There are cases in which we may want both a global and object-based versions for the same metric. For example,
 * regarding SEGMENT_READ_BYTES we publish the global version of it by adding "_global" suffix to the base name
 * ("segmentstore.segment.read_bytes_global") to track the globally total number of bytes read, as well as the
 * per-segment version of it by using the same base name and also supplying additional segment tags
 * ("segmentstore.segment.read_bytes", ["scope", "...", "stream", "...", "segment", "...", "epoch", "..."]) to report
 * in a finer granularity the events read per segment.
 */


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import lombok.Data;

public final class MetricsNames {
    // Metrics in Segment Store Service
    // Segment-related stats
    public static final String SEGMENT_CREATE_LATENCY = "segment.create_latency_ms";              // Histogram, logger: "segmentstore"
    public static final String SEGMENT_DELETE_LATENCY = "segment.delete_latency_ms";              // Histogram, logger: "segmentstore"
    public static final String SEGMENT_READ_LATENCY = "segment.read_latency_ms";                  // Histogram, logger: "segmentstore"
    public static final String SEGMENT_WRITE_LATENCY = "segment.write_latency_ms";                // Histogram, logger: "segmentstore"
    public static final String SEGMENT_READ_BYTES = "segmentstore.segment.read_bytes";            // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_BYTES = "segmentstore.segment.write_bytes";          // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_EVENTS = "segmentstore.segment.write_events";        // Counter and Per-segment Counter

    // Table Segment stats
    public static final String TABLE_SEGMENT_UPDATE_LATENCY = "tablesegment.update_latency_ms";                         // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_UPDATE_CONDITIONAL_LATENCY = "tablesegment.update_conditional_latency_ms"; // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_REMOVE_LATENCY = "tablesegment.remove_latency_ms";                         // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_REMOVE_CONDITIONAL_LATENCY = "tablesegment.remove_conditional_latency_ms"; // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_GET_LATENCY = "tablesegment.get_latency_ms";                               // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_ITERATE_KEYS_LATENCY = "tablesegment.iterate_keys_latency_ms";             // Histogram, logger: "segmentstore"
    public static final String TABLE_SEGMENT_ITERATE_ENTRIES_LATENCY = "tablesegment.iterate_entries_latency_ms";       // Histogram, logger: "segmentstore"

    public static final String TABLE_SEGMENT_UPDATE = "segmentstore.tablesegment.update";                         // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_UPDATE_CONDITIONAL = "segmentstore.tablesegment.update_conditional"; // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_REMOVE = "segmentstore.tablesegment.remove";                         // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_REMOVE_CONDITIONAL = "segmentstore.tablesegment.remove_conditional"; // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_GET = "segmentstore.tablesegment.get";                               // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_ITERATE_KEYS = "segmentstore.tablesegment.iterate_keys";             // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_ITERATE_ENTRIES = "segmentstore.tablesegment.iterate_entries";       // Counter and Per-segment Counter

    // Storage stats
    public static final String STORAGE_READ_LATENCY = "segmentstore.storage.read_latency_ms";   // Histogram, logger: "hdfs" or "filesystem"
    public static final String STORAGE_WRITE_LATENCY = "segmentstore.storage.write_latency_ms"; // Histogram, logger: "hdfs" or "filesystem"
    public static final String STORAGE_READ_BYTES = "segmentstore.storage.read_bytes";          // Counter, logger: "hdfs" or "filesystem"
    public static final String STORAGE_WRITE_BYTES = "segmentstore.storage.write_bytes";        // Counter, logger: "hdfs" or "filesystem"
    public static final String STORAGE_CREATE_COUNT = "segmentstore.storage.create_count";      // Counter, logger: "hdfs" or "filesystem"

    // Cache (RocksDB) stats
    public static final String CACHE_INSERT_LATENCY = "segmentstore.cache.insert_latency_ms";   // Histogram, logger: "rocksdb"
    public static final String CACHE_WRITE_BYTES = "segmentstore.cache.write_bytes";            // Counter
    public static final String CACHE_GET_LATENCY = "segmentstore.cache.get_latency_ms";         // Histogram, logger: "rocksdb"
    public static final String CACHE_READ_BYTES = "segmentstore.cache.read_bytes";              // Counter
    public static final String CACHE_TOTAL_SIZE_BYTES = "segmentstore.cache.size_bytes";        // Gauge
    public static final String CACHE_GENERATION_SPREAD = "cache.gen";                           // Histogram, logger: "segmentstore"

    // DurableDataLog (Tier1) stats
    public static final String BK_TOTAL_WRITE_LATENCY = "segmentstore.total_write_latency_ms";              // Including Queue. Per-container Histogram. Logger: "bookkeeper"
    public static final String BK_WRITE_LATENCY = "segmentstore.write_latency_ms";                          // Exclusively the write to BK. Per-container Histogram. Logger: "bookkeeper"
    public static final String BK_WRITE_BYTES = "bookkeeper.segmentstore.write_bytes";                      // Counter
    public static final String BK_WRITE_QUEUE_SIZE = "segmentstore.write_queue_size";                       // Per-container Histogram. Logger: "bookkeeper"
    public static final String BK_WRITE_QUEUE_FILL_RATE = "segmentstore.write_queue_fill";                  // Per-container Histogram. Logger: "bookkeeper"
    public static final String BK_LEDGER_COUNT = "bookkeeper.segmentstore.bookkeeper_ledger_count";         // Per-container Gauge

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
    public static final String PROCESS_OPERATIONS_LATENCY = "container.process_operations.latency_ms";                 // Per-container Histogram, logger: "segmentstore"
    public static final String PROCESS_OPERATIONS_BATCH_SIZE = "container.process_operations.batch_size";              // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_QUEUE_SIZE = "container.operation_queue.size";                                // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_PROCESSOR_IN_FLIGHT = "container.operation_processor.in_flight";              // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_QUEUE_WAIT_TIME = "container.operation_queue.wait_time";                      // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_PROCESSOR_DELAY_MILLIS = "container.operation_processor.delay_ms";            // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_COMMIT_LATENCY = "container.operation_commit.latency_ms";                     // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_LATENCY = "container.operation.latency_ms";                                   // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_COMMIT_METADATA_TXN_COUNT = "container.operation_commit.metadata_txn_count";  // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_COMMIT_MEMORY_LATENCY = "container.operation_commit.memory_latency_ms";       // Per-container Histogram, logger: "segmentstore"
    public static final String OPERATION_LOG_SIZE = "segmentstore.container.operation.log_size";                       // Per-container Counter

    // Segment container metadata
    public static final String ACTIVE_SEGMENT_COUNT = "segmentstore.container.active_segments";   // Per-container Gauge

    // Thread pool metrics
    public static final String THREAD_POOL_QUEUE_SIZE = "thread_pool.queue_size";          // Histogram, logger: "segmentstore"
    public static final String THREAD_POOL_ACTIVE_THREADS = "thread_pool.active_threads";  // Histogram, logger: "segmentstore"

    // Metrics in Controller
    // Stream request counts
    public static final String CREATE_STREAM = "controller.stream.created";                         // Counter
    public static final String CREATE_STREAM_LATENCY = "stream.created_latency_ms";                 // Histogram, logger: "controller"
    public static final String CREATE_STREAM_FAILED = "controller.stream.create_failed";            // Counter and Per-stream Counter
    public static final String SEAL_STREAM = "controller.stream.sealed";                            // Counter
    public static final String SEAL_STREAM_LATENCY = "stream.sealed_latency_ms";                    // Histogram, logger: "controller"
    public static final String SEAL_STREAM_FAILED = "controller.stream.seal_failed";                // Counter and Per-stream Counter
    public static final String DELETE_STREAM = "controller.stream.deleted";                         // Counter
    public static final String DELETE_STREAM_LATENCY = "stream.deleted_latency_ms";                 // Histogram, logger: "controller"
    public static final String DELETE_STREAM_FAILED = "controller.stream.delete_failed";            // Counter and Per-stream Counter
    public static final String UPDATE_STREAM = "controller.stream.updated";                         // Counter and Per-stream Counter
    public static final String UPDATE_STREAM_LATENCY = "stream.updated_latency_ms";                 // Histogram, logger: "controller"
    public static final String UPDATE_STREAM_FAILED = "controller.stream.update_failed";            // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM = "controller.stream.truncated";                     // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM_LATENCY = "stream.truncated_latency_ms";             // Histogram, logger: "controller"
    public static final String TRUNCATE_STREAM_FAILED = "controller.stream.truncate_failed";        // Counter and Per-stream Counter

    // Transaction request Operations
    public static final String CREATE_TRANSACTION = "controller.transactions.created";                      // Counter and Per-stream Counter
    public static final String CREATE_TRANSACTION_LATENCY = "transactions.created_latency_ms";              // Histogram, logger: "controller"
    public static final String CREATE_TRANSACTION_FAILED = "controller.transactions.create_failed";         // Counter and Per-stream Counter
    public static final String COMMIT_TRANSACTION = "controller.transactions.committed";                    // Counter and Per-stream Counter
    public static final String COMMIT_TRANSACTION_LATENCY = "transactions.committed_latency_ms";            // Histogram, logger: "controller"
    public static final String COMMIT_TRANSACTION_FAILED = "controller.transactions.commit_failed";         // Counter, Per-stream Counter, Per-transaction Counter
    public static final String ABORT_TRANSACTION = "controller.transactions.aborted";                       // Counter and Per-stream Counter
    public static final String ABORT_TRANSACTION_LATENCY = "transactions.aborted_latency_ms";               // Histogram, logger: "controller"
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

    // Zookeeper connectivity metrics
    public static final String CONTROLLER_ZK_SESSION_EXPIRATION = "controller.zookeeper.session_expiration";  // Counter

    public static String globalMetricName(String stringName) {
        return stringName + "_global";
    }

    /**
     * For some metrics such as OpStats, Pravega generates corresponding fail metrics automatically,
     * this method is called to create the name of fail metric for a given metric.
     *
     * Some examples of OpStats metrics and their corresponding fail metrics:
     * pravega.bookkeeper.segmentstore.write_latency_ms.0
     * pravega.bookkeeper.segmentstore.write_latency_ms_fail.0
     *
     * pravega.segmentstore.thread_pool.active_threads
     * pravega.segmentstore.thread_pool.active_threads_fail
     *
     * The rule is, if the last segment of the metric is an integer, such as container id, the suffix "_fail"
     * is appended to the preceeding segment instead of the integer itself; otherwise simply append "_fail"
     * onto the given metric to get the fail metric.
     *
     * @param metricName the metric name for which fail metric is created
     * @return the name of fail metric
     */
    public static String failMetricName(String metricName) {
        if (Strings.isNullOrEmpty(metricName)) {
            return metricName;
        }
        String[] tags = metricName.split("\\.");
        if (tags.length >= 2 && Ints.tryParse(tags[tags.length - 1]) != null) {
            tags[tags.length - 2] += "_fail";
            return String.join(".", tags);
        } else {
            return metricName + "_fail";
        }
    }

    /**
     * Convenient method to join two Strings with dot.
     *
     * @param element1 the first String object.
     * @param element2 the second String object.
     * @return String object with element1 and element2 joined with dot.
     */
    public static String joinWithDot(String element1, String element2) {
        return element1 + "." + element2;
    }

    /**
     * Create an MetricKey object based on metric name, metric type and tags associated.
     * The MetricKey object contains cache key for cache lookup and registry key for registry lookup.
     *
     * @param metric the metric name.
     * @param tags the tag(s) associated with the metric.
     * @return the MetricKey object contains cache lookup key and metric registry key.
     */
    public static MetricKey metricKey(String metric, String... tags) {

        if (tags == null || tags.length == 0) {  //if no tags supplied, the original metric name is used for both cache key and registry key.
            return new MetricKey(metric, metric);
        } else { //if tag is supplied, append tag value to form cache key; original metric name is registry key.
            StringBuilder sb = new StringBuilder(metric);
            Preconditions.checkArgument((tags.length % 2) == 0, "Tags is a set of key/value pair so the size must be even: %s", tags.length);
            for (int i = 0; i < tags.length; i += 2) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(tags[i]) || !Strings.isNullOrEmpty(tags[i + 1]), "Tag name or value cannot be empty or null");
                sb.append('.').append(tags[i + 1]);
            }
            return new MetricKey(sb.toString(), metric);
        }
    }

    /**
     * MetricKey is a place holder to hold metric cache lookup key and registry key.
     * This class is provided to keep the metric name convention backwards compatible.
     *
     */
    @Data
    public static class MetricKey {
        private final String cacheKey;
        private final String registryKey;
    }
}
