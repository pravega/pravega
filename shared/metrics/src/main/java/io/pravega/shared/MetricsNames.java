/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
 * All metric names are in the format of
 * "Metrics Prefix + Component Origin + Sub-Component (or Abstraction) + Metric Base Name":
 *
 *     Metric Prefix: "pravega" by default, configurable.
 *     Component Origin: indicates which component generates the metric, such as "segmentstore", "controller".
 *     Sub-Component (or Abstraction): 2nd level component or abstraction, such as "cache", "transaction", "storage".
 *     Metric Base Name: such as "read_latency_ms", "create_count".
 *
 * Some common combinations of component and sub-components (or abstractions) being used are:
 * - segmentstore.segment: metrics for individual segments
 * - segmentstore.storage: metrics related to our long-term storage (Tier 2)
 * - segmentstore.bookkeeper: metrics related to bookkeeper (Tier 1)
 * - segmentstore.container: metrics for segment containers
 * - segmentstore.thread_pool: metrics for segmentstore thread pool
 * - segmentstore.cache: cache-related metrics
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
 *   containerId measured, with different container tag (e.g. ["containerId", "3"]).
 *
 * There are cases in which we may want both a global and object-based versions for the same metric. For example,
 * regarding SEGMENT_READ_BYTES we publish the global version of it by adding "_global" suffix to the base name
 * ("segmentstore.segment.read_bytes_global") to track the globally total number of bytes read, as well as the
 * per-segment version of it by using the same base name and also supplying additional segment tags
 * ("segmentstore.segment.read_bytes", ["scope", "...", "stream", "...", "segment", "...", "epoch", "..."])
 * to report in a finer granularity the events read per segment.
 */


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import lombok.Data;

public final class MetricsNames {

    // PREFIX can be loaded from config when config becomes dependency.
    public static final String PREFIX = "pravega" + ".";

    // Metrics for all services
    public static final String LOG_ERRORS = PREFIX + "logged_errors";     // Counter
    public static final String LOG_WARNINGS = PREFIX + "logged_warnings"; // Counter

    // Metrics in Segment Store Service
    // Segment-related stats
    public static final String SEGMENT_CREATE_LATENCY = PREFIX + "segmentstore.segment.create_latency_ms";              // Histogram
    public static final String SEGMENT_DELETE_LATENCY = PREFIX + "segmentstore.segment.delete_latency_ms";              // Histogram
    public static final String SEGMENT_READ_LATENCY = PREFIX + "segmentstore.segment.read_latency_ms";                  // Histogram
    public static final String SEGMENT_WRITE_LATENCY = PREFIX + "segmentstore.segment.write_latency_ms";                // Histogram
    public static final String SEGMENT_APPEND_SIZE = PREFIX + "segmentstore.segment.append_size";                       // Histogram
    public static final String SEGMENT_READ_SIZE = PREFIX + "segmentstore.segment.read_size";                           // Histogram
    public static final String SEGMENT_READ_BYTES = PREFIX + "segmentstore.segment.read_bytes";                         // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_BYTES = PREFIX + "segmentstore.segment.write_bytes";                       // Counter and Per-segment Counter
    public static final String SEGMENT_WRITE_EVENTS = PREFIX + "segmentstore.segment.write_events";                     // Counter and Per-segment Counter

    // Table Segment stats
    public static final String TABLE_SEGMENT_UPDATE_LATENCY = PREFIX + "segmentstore.tablesegment.update_latency_ms";                         // Histogram
    public static final String TABLE_SEGMENT_UPDATE_CONDITIONAL_LATENCY = PREFIX + "segmentstore.tablesegment.update_conditional_latency_ms"; // Histogram
    public static final String TABLE_SEGMENT_REMOVE_LATENCY = PREFIX + "segmentstore.tablesegment.remove_latency_ms";                         // Histogram
    public static final String TABLE_SEGMENT_REMOVE_CONDITIONAL_LATENCY = PREFIX + "segmentstore.tablesegment.remove_conditional_latency_ms"; // Histogram
    public static final String TABLE_SEGMENT_GET_LATENCY = PREFIX + "segmentstore.tablesegment.get_latency_ms";                               // Histogram
    public static final String TABLE_SEGMENT_ITERATE_KEYS_LATENCY = PREFIX + "segmentstore.tablesegment.iterate_keys_latency_ms";             // Histogram
    public static final String TABLE_SEGMENT_ITERATE_ENTRIES_LATENCY = PREFIX + "segmentstore.tablesegment.iterate_entries_latency_ms";       // Histogram

    public static final String TABLE_SEGMENT_UPDATE = PREFIX + "segmentstore.tablesegment.update";                         // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_UPDATE_CONDITIONAL = PREFIX + "segmentstore.tablesegment.update_conditional"; // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_REMOVE = PREFIX + "segmentstore.tablesegment.remove";                         // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_REMOVE_CONDITIONAL = PREFIX + "segmentstore.tablesegment.remove_conditional"; // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_GET = PREFIX + "segmentstore.tablesegment.get";                               // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_ITERATE_KEYS = PREFIX + "segmentstore.tablesegment.iterate_keys";             // Counter and Per-segment Counter
    public static final String TABLE_SEGMENT_ITERATE_ENTRIES = PREFIX + "segmentstore.tablesegment.iterate_entries";       // Counter and Per-segment Counter

    // Storage stats
    public static final String STORAGE_READ_LATENCY = PREFIX + "segmentstore.storage.read_latency_ms";     // Histogram
    public static final String STORAGE_WRITE_LATENCY = PREFIX + "segmentstore.storage.write_latency_ms";   // Histogram
    public static final String STORAGE_CREATE_LATENCY = PREFIX + "segmentstore.storage.create_latency_ms"; // Histogram
    public static final String STORAGE_DELETE_LATENCY = PREFIX + "segmentstore.storage.delete_latency_ms"; // Histogram
    public static final String STORAGE_CONCAT_LATENCY = PREFIX + "segmentstore.storage.concat_latency_ms"; // Histogram

    public static final String STORAGE_READ_BYTES = PREFIX + "segmentstore.storage.read_bytes";          // Counter
    public static final String STORAGE_WRITE_BYTES = PREFIX + "segmentstore.storage.write_bytes";        // Counter
    public static final String STORAGE_CONCAT_BYTES = PREFIX + "segmentstore.storage.concat_bytes";      // Counter
    public static final String STORAGE_CREATE_COUNT = PREFIX + "segmentstore.storage.create_count";      // Counter
    public static final String STORAGE_DELETE_COUNT = PREFIX + "segmentstore.storage.delete_count";      // Counter
    public static final String STORAGE_CONCAT_COUNT = PREFIX + "segmentstore.storage.concat_count";      // Counter
    public static final String STORAGE_LARGE_CONCAT_COUNT = PREFIX + "segmentstore.storage.large_concat_count"; // Counter

    // Cache stats
    public static final String CACHE_WRITE_BYTES = PREFIX + "segmentstore.cache.write_bytes";                                     // Counter
    public static final String CACHE_APPEND_BYTES = PREFIX + "segmentstore.cache.append_bytes";                                   // Counter
    public static final String CACHE_READ_BYTES = PREFIX + "segmentstore.cache.read_bytes";                                       // Counter
    public static final String CACHE_DELETE_BYTES = PREFIX + "segmentstore.cache.delete_bytes";                                   // Counter
    public static final String CACHE_STORED_SIZE_BYTES = PREFIX + "segmentstore.cache.stored_size_bytes";                         // Gauge
    public static final String CACHE_USED_SIZE_BYTES = PREFIX + "segmentstore.cache.used_size_bytes";                             // Gauge
    public static final String CACHE_ALLOC_SIZE_BYTES = PREFIX + "segmentstore.cache.allocated_size_bytes";                       // Gauge
    public static final String CACHE_GENERATION_SPREAD = PREFIX + "segmentstore.cache.gen";                                       // Histogram
    public static final String CACHE_MANAGER_ITERATION_DURATION = PREFIX + "segmentstore.cache.manager_iteration_duration_ms";    // Timer

    // DurableDataLog (Tier1) stats
    public static final String BK_TOTAL_WRITE_LATENCY = PREFIX + "segmentstore.bookkeeper.total_write_latency_ms";   // Including Queue. Per-container Histogram
    public static final String BK_WRITE_LATENCY = PREFIX + "segmentstore.bookkeeper.write_latency_ms";               // Exclusively the write to BK. Per-container Histogram
    public static final String BK_WRITE_BYTES = PREFIX + "segmentstore.bookkeeper.write_bytes";                      // Counter
    public static final String BK_WRITE_QUEUE_SIZE = PREFIX + "segmentstore.bookkeeper.write_queue_size";            // Per-container Histogram
    public static final String BK_WRITE_QUEUE_FILL_RATE = PREFIX + "segmentstore.bookkeeper.write_queue_fill";       // Per-container Histogram
    public static final String BK_LEDGER_COUNT = PREFIX + "segmentstore.bookkeeper.ledger_count";                    // Per-container Gauge

    // StorageWriter stats
    public static final String STORAGE_WRITER_FLUSH_ELAPSED = PREFIX + "segmentstore.storagewriter.flush_elapsed_ms";         // Time to flush all processors. Per-container Histogram.
    public static final String STORAGE_WRITER_ITERATION_ELAPSED = PREFIX + "segmentstore.storagewriter.iteration_elapsed_ms"; // Total Iteration Duration. Per-container Histogram
    public static final String STORAGE_WRITER_READ_COUNT = PREFIX + "segmentstore.storagewriter.read_count";                  // Items read from DurableLog. Counter.
    public static final String STORAGE_WRITER_FLUSHED_BYTES = PREFIX + "segmentstore.storagewriter.flushed_bytes";            // Bytes written per iteration. Counter.
    public static final String STORAGE_WRITER_MERGED_BYTES = PREFIX + "segmentstore.storagewriter.merged_bytes";              // Bytes merged per iteration. Counter.
    public static final String STORAGE_WRITER_FLUSHED_ATTRIBUTES = PREFIX + "segmentstore.storagewriter.flushed_attributes";  // Attributes flushed per iteration. Counter.

    // Segment container metrics
    public static final String CONTAINER_APPEND_COUNT = PREFIX + "segmentstore.container.append_count";                          // Per-container Event Counter
    public static final String CONTAINER_APPEND_OFFSET_COUNT = PREFIX + "segmentstore.container.append_offset_count";            // Per-container Event Counter
    public static final String CONTAINER_UPDATE_ATTRIBUTES_COUNT = PREFIX + "segmentstore.container.update_attributes_count";    // Per-container Event Counter
    public static final String CONTAINER_GET_ATTRIBUTES_COUNT = PREFIX + "segmentstore.container.get_attributes_count";          // Per-container Event Counter
    public static final String CONTAINER_READ_COUNT = PREFIX + "segmentstore.container.read_count";                              // Per-container Event Counter
    public static final String CONTAINER_GET_INFO_COUNT = PREFIX + "segmentstore.container.get_info_count";                      // Per-container Event Counter
    public static final String CONTAINER_CREATE_SEGMENT_COUNT = PREFIX + "segmentstore.container.create_segment_count";          // Per-container Event Counter
    public static final String CONTAINER_DELETE_SEGMENT_COUNT = PREFIX + "segmentstore.container.delete_segment_count";          // Per-container Event Counter
    public static final String CONTAINER_MERGE_SEGMENT_COUNT = PREFIX + "segmentstore.container.merge_segment_count";            // Per-container Event Counter
    public static final String CONTAINER_SEAL_COUNT = PREFIX + "segmentstore.container.seal_count";                              // Per-container Event Counter
    public static final String CONTAINER_TRUNCATE_COUNT = PREFIX + "segmentstore.container.truncate_count";                      // Per-container Event Counter
    public static final String CONTAINER_RECOVERY_TIME = PREFIX + "segmentstore.container.recovery_time";                        // Per-container Gauge

    // Operation processor metrics
    public static final String PROCESS_OPERATIONS_LATENCY = PREFIX + "segmentstore.container.process_operations.latency_ms";                 // Per-container Histogram
    public static final String PROCESS_OPERATIONS_BATCH_SIZE = PREFIX + "segmentstore.container.process_operations.batch_size";              // Per-container Histogram
    public static final String OPERATION_QUEUE_SIZE = PREFIX + "segmentstore.container.operation_queue.size";                                // Per-container Histogram
    public static final String OPERATION_PROCESSOR_IN_FLIGHT = PREFIX + "segmentstore.container.operation_processor.in_flight";              // Per-container Histogram
    public static final String OPERATION_QUEUE_WAIT_TIME = PREFIX + "segmentstore.container.operation_queue.wait_time";                      // Per-container Histogram
    public static final String OPERATION_PROCESSOR_DELAY_MILLIS = PREFIX + "segmentstore.container.operation_processor.delay_ms";            // Per-container Histogram
    public static final String OPERATION_COMMIT_LATENCY = PREFIX + "segmentstore.container.operation_commit.latency_ms";                     // Per-container Histogram
    public static final String OPERATION_LATENCY = PREFIX + "segmentstore.container.operation.latency_ms";                                   // Per-container Histogram
    public static final String OPERATION_COMMIT_MEMORY_COUNT = PREFIX + "segmentstore.container.operation_commit.memory_count";              // Per-container Histogram
    public static final String OPERATION_COMMIT_MEMORY_LATENCY = PREFIX + "segmentstore.container.operation_commit.memory_latency_ms";       // Per-container Histogram
    public static final String OPERATION_LOG_SIZE = PREFIX + "segmentstore.container.operation.log_size";                                    // Per-container Counter

    // Segment container metadata
    public static final String ACTIVE_SEGMENT_COUNT = PREFIX + "segmentstore.container.active_segments";   // Per-container Gauge

    // Thread pool metrics
    public static final String THREAD_POOL_QUEUE_SIZE = PREFIX + "segmentstore.thread_pool.queue_size";          // Histogram
    public static final String THREAD_POOL_ACTIVE_THREADS = PREFIX + "segmentstore.thread_pool.active_threads";  // Histogram

    // Metrics in Controller
    // KeyValueTable create request counts
    public static final String CREATE_KVTABLE = PREFIX + "controller.kvtable.created";
    // KeyValueTable segment counts
    public static final String KVTABLE_SEGMENTS_COUNT = PREFIX + "controller.kvtable.segments.count";
    // KeyValueTable Create Latency
    public static final String CREATE_KVTABLE_LATENCY = PREFIX + "controller.kvtable.created_latency_ms";
    // KeyValueTable Create Failure
    public static final String CREATE_KVTABLE_FAILED = PREFIX + "controller.kvtable.create_failed";
    public static final String DELETE_KVTABLE = PREFIX + "controller.kvtable.deleted";                         // Counter
    public static final String DELETE_KVTABLE_LATENCY = PREFIX + "controller.kvtable.deleted_latency_ms";      // Histogram
    public static final String DELETE_KVTABLE_FAILED = PREFIX + "controller.kvtable.delete_failed";

    // Scope request counts
    public static final String CREATE_SCOPE = PREFIX + "controller.scope.created";                           // Counter
    public static final String CREATE_SCOPE_LATENCY = PREFIX + "controller.scope.created_latency_ms";        // Histogram
    public static final String CREATE_SCOPE_FAILED = PREFIX + "controller.scope.create_failed";              // Counter and Per-scope Counter
    public static final String DELETE_SCOPE = PREFIX + "controller.scope.deleted";                           // Counter
    public static final String DELETE_SCOPE_LATENCY = PREFIX + "controller.scope.deleted_latency_ms";        // Histogram
    public static final String DELETE_SCOPE_FAILED = PREFIX + "controller.scope.delete_failed";              // Counter and Per-scope Counter

    // Stream request counts
    public static final String CREATE_STREAM = PREFIX + "controller.stream.created";                         // Counter
    public static final String CREATE_STREAM_LATENCY = PREFIX + "controller.stream.created_latency_ms";      // Histogram
    public static final String CREATE_STREAM_FAILED = PREFIX + "controller.stream.create_failed";            // Counter and Per-stream Counter
    public static final String SEAL_STREAM = PREFIX + "controller.stream.sealed";                            // Counter
    public static final String SEAL_STREAM_LATENCY = PREFIX + "controller.stream.sealed_latency_ms";         // Histogram
    public static final String SEAL_STREAM_FAILED = PREFIX + "controller.stream.seal_failed";                // Counter and Per-stream Counter
    public static final String DELETE_STREAM = PREFIX + "controller.stream.deleted";                         // Counter
    public static final String DELETE_STREAM_LATENCY = PREFIX + "controller.stream.deleted_latency_ms";      // Histogram
    public static final String DELETE_STREAM_FAILED = PREFIX + "controller.stream.delete_failed";            // Counter and Per-stream Counter
    public static final String UPDATE_STREAM = PREFIX + "controller.stream.updated";                         // Counter and Per-stream Counter
    public static final String UPDATE_STREAM_LATENCY = PREFIX + "controller.stream.updated_latency_ms";      // Histogram
    public static final String UPDATE_STREAM_FAILED = PREFIX + "controller.stream.update_failed";            // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM = PREFIX + "controller.stream.truncated";                     // Counter and Per-stream Counter
    public static final String TRUNCATE_STREAM_LATENCY = PREFIX + "controller.stream.truncated_latency_ms";  // Histogram
    public static final String TRUNCATE_STREAM_FAILED = PREFIX + "controller.stream.truncate_failed";        // Counter and Per-stream Counter
    public static final String ADD_SUBSCRIBER = PREFIX + "controller.stream.add_subscriber";                     // Counter and Per-stream Counter
    public static final String ADD_SUBSCRIBER_LATENCY = PREFIX + "controller.stream.add_subscriber_latency_ms";  // Histogram
    public static final String ADD_SUBSCRIBER_FAILED = PREFIX + "controller.stream.add_subscriber_failed";        // Counter and Per-stream Counter
    public static final String REMOVE_SUBSCRIBER = PREFIX + "controller.stream.remove_subscriber";                     // Counter and Per-stream Counter
    public static final String REMOVE_SUBSCRIBER_LATENCY = PREFIX + "controller.stream.remove_subscriber_latency_ms";  // Histogram
    public static final String REMOVE_SUBSCRIBER_FAILED = PREFIX + "controller.stream.remove_subscriber_failed";        // Counter and Per-stream Counter
    public static final String UPDATE_SUBSCRIBER = PREFIX + "controller.stream.update_subscriber";                     // Counter and Per-stream Counter
    public static final String UPDATE_SUBSCRIBER_LATENCY = PREFIX + "controller.stream.update_subscriber_latency_ms";  // Histogram
    public static final String UPDATE_SUBSCRIBER_FAILED = PREFIX + "controller.stream.update_subscriber_failed";        // Counter and Per-stream Counter

    // Transaction request Operations
    public static final String CREATE_TRANSACTION = PREFIX + "controller.transactions.created";                                         // Counter and Per-stream Counter
    public static final String CREATE_TRANSACTION_LATENCY = PREFIX + "controller.transactions.created_latency_ms";                      // Histogram
    public static final String CREATE_TRANSACTION_SEGMENTS_LATENCY = PREFIX + "controller.transactions.created_segments_latency_ms";    // Histogram
    public static final String CREATE_TRANSACTION_FAILED = PREFIX + "controller.transactions.create_failed";                            // Counter and Per-stream Counter
    public static final String COMMITTING_TRANSACTION_LATENCY = PREFIX + "controller.transactions.committing_latency_ms";               // Histogram
    public static final String COMMIT_TRANSACTION = PREFIX + "controller.transactions.committed";                                       // Counter and Per-stream Counter
    public static final String COMMIT_TRANSACTION_LATENCY = PREFIX + "controller.transactions.committed_latency_ms";                    // Histogram
    public static final String COMMIT_TRANSACTION_SEGMENTS_LATENCY = PREFIX + "controller.transactions.committed_segments_latency_ms";  // Histogram
    public static final String COMMIT_TRANSACTION_FAILED = PREFIX + "controller.transactions.commit_failed";                            // Counter, Per-stream Counter, Per-transaction Counter
    public static final String ABORTING_TRANSACTION_LATENCY = PREFIX + "controller.transactions.aborting_latency_ms";                   // Histogram
    public static final String ABORT_TRANSACTION = PREFIX + "controller.transactions.aborted";                                          // Counter and Per-stream Counter
    public static final String ABORT_TRANSACTION_LATENCY = PREFIX + "controller.transactions.aborted_latency_ms";                       // Histogram
    public static final String ABORT_TRANSACTION_SEGMENTS_LATENCY = PREFIX + "controller.transactions.aborted_segments_latency_ms";     // Histogram
    public static final String ABORT_TRANSACTION_FAILED = PREFIX + "controller.transactions.abort_failed";                              // Counter, Per-stream Counter, Per-transaction Counter
    public static final String OPEN_TRANSACTIONS = PREFIX + "controller.transactions.opened";                                           // Per-stream Gauge
    public static final String TIMEDOUT_TRANSACTIONS = PREFIX + "controller.transactions.timedout";                                     // Per-stream Counter

    // Host metrics
    public static final String SEGMENT_STORE_HOST_NUMBER = PREFIX + "controller.hosts.count";                    // Gauge
    public static final String SEGMENT_STORE_HOST_FAILURES = PREFIX + "controller.hosts.failures";               // Counter and Per-host counter
    public static final String SEGMENT_STORE_HOST_CONTAINER_COUNT = PREFIX + "controller.hosts.container_count"; // Per-host Gauge

    // Container lifecycle metrics
    public static final String CONTAINER_FAILOVERS = PREFIX + "controller.container.failovers";    // Counter and Per-container Counter

    // Stream segment counts
    public static final String SEGMENTS_COUNT = PREFIX + "controller.segments.count";    // Per-stream Gauge
    public static final String SEGMENTS_SPLITS = PREFIX + "controller.segment.splits";   // Per-stream Gauge
    public static final String SEGMENTS_MERGES = PREFIX + "controller.segment.merges";   // Per-stream Gauge

    // Stream retention operations
    public static final String RETENTION_FREQUENCY = PREFIX + "controller.retention.frequency";   // Per-stream Counter
    public static final String TRUNCATED_SIZE = PREFIX + "controller.retention.truncated_size";   // Per-stream Gauge

    // Zookeeper connectivity metrics
    public static final String CONTROLLER_ZK_SESSION_EXPIRATION = PREFIX + "controller.zookeeper.session_expiration";  // Counter

    public static String globalMetricName(String stringName) {
        return stringName + "_global";
    }

    /**
     * For some metrics such as OpStats, Pravega generates corresponding fail metrics automatically,
     * this method is called to create the name of fail metric for a given metric.
     *
     * Some examples of OpStats metrics and their corresponding fail metrics:
     * pravega.segmentstore.bookkeeper.write_latency_ms.0
     * pravega.segmentstore.bookkeeper.write_latency_ms_fail.0
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
