/**
 * Copyright Pravega Authors.
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
package io.pravega.controller.metrics;

import com.google.common.base.Preconditions;
import io.pravega.shared.metrics.OpStatsLogger;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.shared.MetricsNames.CREATE_SCOPE;
import static io.pravega.shared.MetricsNames.CREATE_SCOPE_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_SCOPE_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE_RECURSIVE_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_STREAM;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.RETENTION_FREQUENCY;
import static io.pravega.shared.MetricsNames.SEAL_STREAM;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENTS_MERGES;
import static io.pravega.shared.MetricsNames.SEGMENTS_SPLITS;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE;
import static io.pravega.shared.MetricsNames.KVTABLE_SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_READER_GROUP_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_READER_GROUP_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_READER_GROUP;
import static io.pravega.shared.MetricsNames.CREATE_READER_GROUP_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_READER_GROUP;
import static io.pravega.shared.MetricsNames.DELETE_READER_GROUP_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_READER_GROUP_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_READER_GROUP;
import static io.pravega.shared.MetricsNames.UPDATE_READER_GROUP_FAILED;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_AUTO_SCALE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_CREATE_READER_GROUP_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_CREATE_TABLE_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_READER_GROUP_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_SCOPE_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_TABLE_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_SCALE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_UPDATE_READER_GROUP_LATENCY;

import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.shared.MetricsTags.readerGroupTags;

/**
 * Class to encapsulate the logic to report Controller service metrics for Streams.
 */
public final class StreamMetrics extends AbstractControllerMetrics {

    @SuppressWarnings("MatchXpath")
    private static final AtomicReference<StreamMetrics> INSTANCE = new AtomicReference<>();

    private final OpStatsLogger createStreamLatency;
    private final OpStatsLogger deleteStreamLatency;
    private final OpStatsLogger sealStreamLatency;
    private final OpStatsLogger updateStreamLatency;
    private final OpStatsLogger addReaderGroupLatency;
    private final OpStatsLogger truncateStreamLatency;
    private final OpStatsLogger deleteReaderGroupLatency;
    private final OpStatsLogger updateReaderGroupLatency;
    private final OpStatsLogger updateSubscriberLatency;
    private final OpStatsLogger createKeyValueTableLatency;
    private final OpStatsLogger deleteKeyValueTableLatency;

    private final OpStatsLogger createScopeLatency;
    private final OpStatsLogger deleteScopeLatency;
    // Controller EventProcessor events latency
    private final OpStatsLogger controllerEventProcessorDeleteStreamLatency;
    private final OpStatsLogger controllerEventProcessorUpdateStreamLatency;
    private final OpStatsLogger controllerEventProcessorSealStreamLatency;
    private final OpStatsLogger controllerEventProcessorTruncateStreamLatency;
    private final OpStatsLogger controllerEventProcessorScaleStreamLatency;
    private final OpStatsLogger controllerEventProcessorAutoScaleStreamLatency;
    private final OpStatsLogger controllerEventProcessorDeleteScopeLatency;
    private final OpStatsLogger controllerEventProcessorCreateReaderGroupLatency;
    private final OpStatsLogger controllerEventProcessorDeleteReaderGroupLatency;
    private final OpStatsLogger controllerEventProcessorUpdateReaderGroupLatency;
    private final OpStatsLogger controllerEventProcessorCreateTableLatency;
    private final OpStatsLogger controllerEventProcessorDeleteTableLatency;

    private StreamMetrics() {
        createStreamLatency = STATS_LOGGER.createStats(CREATE_STREAM_LATENCY);
        deleteStreamLatency = STATS_LOGGER.createStats(DELETE_STREAM_LATENCY);
        sealStreamLatency = STATS_LOGGER.createStats(SEAL_STREAM_LATENCY);
        updateStreamLatency = STATS_LOGGER.createStats(UPDATE_STREAM_LATENCY);
        truncateStreamLatency = STATS_LOGGER.createStats(TRUNCATE_STREAM_LATENCY);
        addReaderGroupLatency = STATS_LOGGER.createStats(CREATE_READER_GROUP_LATENCY);
        deleteReaderGroupLatency = STATS_LOGGER.createStats(DELETE_READER_GROUP_LATENCY);
        createScopeLatency = STATS_LOGGER.createStats(CREATE_SCOPE_LATENCY);
        deleteScopeLatency = STATS_LOGGER.createStats(DELETE_SCOPE_LATENCY);
        createKeyValueTableLatency = STATS_LOGGER.createStats(CREATE_KVTABLE_LATENCY);
        deleteKeyValueTableLatency = STATS_LOGGER.createStats(DELETE_KVTABLE_LATENCY);
        updateSubscriberLatency = STATS_LOGGER.createStats(UPDATE_SUBSCRIBER_LATENCY);
        updateReaderGroupLatency = STATS_LOGGER.createStats(UPDATE_READER_GROUP_LATENCY);

        controllerEventProcessorDeleteStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_DELETE_STREAM_LATENCY);
        controllerEventProcessorUpdateStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_UPDATE_STREAM_LATENCY);
        controllerEventProcessorSealStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_SEAL_STREAM_LATENCY);
        controllerEventProcessorTruncateStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_TRUNCATE_STREAM_LATENCY);
        controllerEventProcessorScaleStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_SCALE_STREAM_LATENCY);
        controllerEventProcessorAutoScaleStreamLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_AUTO_SCALE_STREAM_LATENCY);
        controllerEventProcessorDeleteScopeLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_DELETE_SCOPE_LATENCY);
        controllerEventProcessorCreateReaderGroupLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_CREATE_READER_GROUP_LATENCY);
        controllerEventProcessorDeleteReaderGroupLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_DELETE_READER_GROUP_LATENCY);
        controllerEventProcessorUpdateReaderGroupLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_UPDATE_READER_GROUP_LATENCY);
        controllerEventProcessorCreateTableLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_CREATE_TABLE_LATENCY);
        controllerEventProcessorDeleteTableLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_DELETE_TABLE_LATENCY);
    }

    /**
     * Mandatory call to initialize the singleton object.
     */
    public static synchronized void initialize() {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new StreamMetrics());
        }
    }

    /**
     * Get the singleton {@link StreamMetrics} instance. It is mandatory to call initialize before invoking this method.
     *
     * @return StreamMetrics instance.
     */
    public static StreamMetrics getInstance() {
        Preconditions.checkState(INSTANCE.get() != null, "You need call initialize before using this class.");
        return INSTANCE.get();
    }

    /**
     * This method increments the global and KeyValueTable-specific counters of KeyValueTable creations,
     * initializes other kvt-specific metrics and reports the latency of the operation.
     *
     * @param scope             Scope.
     * @param kvtName           Name of the KeyValueTable.
     * @param minNumSegments    Initial number of segments for the KeyValueTable.
     * @param latency           Latency of the create KeyValueTable operation.
     */
    public void createKeyValueTable(String scope, String kvtName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_KVTABLE, 1);
        DYNAMIC_LOGGER.reportGaugeValue(KVTABLE_SEGMENTS_COUNT, minNumSegments, streamTags(scope, kvtName));
        createKeyValueTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of failed KeyValueTable creations in the system as well as the failed creation
     * attempts for this specific KeyValueTable.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     */
    public void createKeyValueTableFailed(String scope, String kvtName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_KVTABLE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_KVTABLE_FAILED, 1, streamTags(scope, kvtName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream deletions and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     * @param latency       Latency of the deleteKeyValueTable
     *                      operation.
     */
    public void deleteKeyValueTable(String scope, String kvtName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_KVTABLE, 1);
        deleteKeyValueTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     */
    public void deleteKeyValueTableFailed(String scope, String kvtName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_KVTABLE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_KVTABLE_FAILED, 1, streamTags(scope, kvtName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream creations, initializes other
     * stream-specific metrics and reports the latency of the operation.
     *
     * @param scope             Scope.
     * @param streamName        Name of the Stream.
     * @param minNumSegments    Initial number of segments for the Stream.
     * @param latency           Latency of the createStream operation.
     */
    public void createStream(String scope, String streamName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM, 1);
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0, streamTags(scope, streamName));
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_COUNT, minNumSegments, streamTags(scope, streamName));
        createStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of Scope creations and reports the latency of the operation.
     *
     * @param latency           Latency of the createStream operation.
     */
    public void createScope(Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_SCOPE, 1);
        createScopeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of failed Stream creations in the system as well as the failed creation
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void createStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global counter of failed Scope creations in the system as well as the failed creation
     * attempts for this specific Scope.
     *
     * @param scope         Scope.
     */
    public void createScopeFailed(String scope) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_SCOPE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_SCOPE_FAILED, 1, streamTags(scope, ""));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream deletions and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the deleteStream operation.
     */
    public void deleteStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM, 1);
        deleteStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor delete Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorDeleteStreamEvent operation.
     */
    public void controllerEventProcessorDeleteStreamEvent(Duration latency) {
        controllerEventProcessorDeleteStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor update Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorUpdateStreamEvent operation.
     */
    public void controllerEventProcessorUpdateStreamEvent(Duration latency) {
        controllerEventProcessorUpdateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor seal Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorSealStreamEvent operation.
     */
    public void controllerEventProcessorSealStreamEvent(Duration latency) {
        controllerEventProcessorSealStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor truncate Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorTruncateStreamEvent operation.
     */
    public void controllerEventProcessorTruncateStreamEvent(Duration latency) {
        controllerEventProcessorTruncateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor autoScale Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorAutoScaleStreamLatency operation.
     */
    public void controllerEventProcessorAutoScaleStreamEvent(Duration latency) {
        controllerEventProcessorAutoScaleStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor createReaderGroup processing.
     *
     * @param latency       Latency of the controllerEventProcessorCreateReaderGroupEvent operation.
     */
    public void controllerEventProcessorCreateReaderGroupEvent(Duration latency) {
        controllerEventProcessorCreateReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor deleteReaderGroup event processing.
     *
     * @param latency       Latency of the controllerEventProcessorDeleteReaderGroupEvent operation.
     */
    public void controllerEventProcessorDeleteReaderGroupEvent(Duration latency) {
        controllerEventProcessorDeleteReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor updateReaderGroup event processing.
     *
     * @param latency       Latency of the controllerEventProcessorUpdateReaderGroupEvent operation.
     */
    public void controllerEventProcessorUpdateReaderGroupEvent(Duration latency) {
        controllerEventProcessorUpdateReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor deleteScope event processing.
     *
     * @param latency       Latency of the controllerEventProcessorDeleteScopeEvent operation.
     */
    public void controllerEventProcessorDeleteScopeEvent(Duration latency) {
        controllerEventProcessorDeleteScopeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of ControllerEventProcessor scale Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorScaleStreamLatency operation.
     */
    public void controllerEventProcessorScaleStreamEvent(Duration latency) {
        controllerEventProcessorScaleStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of createTable  Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorCreateTableLatency operation.
     */
    public void controllerEventProcessorCreateTableEvent(Duration latency) {
        controllerEventProcessorCreateTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of deleteTable Stream event processing.
     *
     * @param latency       Latency of the controllerEventProcessorDeleteTableLatency operation.
     */
    public void controllerEventProcessorDeleteTableEvent(Duration latency) {
        controllerEventProcessorDeleteTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of Scope deletions and reports the latency of the operation.
     *
     * @param latency       Latency of the deleteStream operation.
     */
    public void deleteScope(Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_SCOPE, 1);
        deleteScopeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void deleteStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     */
    public void deleteScopeFailed(String scope) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_SCOPE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_SCOPE_FAILED, 1, streamTags(scope, ""));
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     */
    public void deleteScopeRecursiveFailed(String scope) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_SCOPE_RECURSIVE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_SCOPE_RECURSIVE_FAILED, 1, streamTags(scope, ""));
    }

    /**
     * This method increments the global and Stream-specific counters of seal Stream operations, set the number of open
     * Transactions to 0, and reports the latency of the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the sealStream operation.
     */
    public void sealStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM, 1);
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0, streamTags(scope, streamName));
        sealStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream seal operations in the system as well as the failed seal
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void sealStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEAL_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream updates and reports the latency of the
     * operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the updateStream operation.
     */
    public void updateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_STREAM), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM, 1, streamTags(scope, streamName));
        updateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream update operations in the system as well as the failed
     * update attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void updateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and ReaderGroup-specific counters for createReaderGroup operation
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param rgName        Name of the Reader Group.
     * @param latency       Latency of the createReaderGroup operation.
     */
    public void createReaderGroup(String scope, String rgName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_READER_GROUP), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_READER_GROUP, 1, readerGroupTags(scope, rgName));
        addReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed createReaderGroup operation.
     *
     * @param scope         Scope Name.
     * @param rgName        Reader Group Name.
     */
    public void createReaderGroupFailed(String scope, String rgName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_READER_GROUP_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_READER_GROUP_FAILED, 1, readerGroupTags(scope, rgName));
    }

    /**
     * This method increments the global and ReaderGroup-specific counters for updateReaderGroup operation
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param rgName        Name of the ReaderGroup.
     * @param latency       Latency of the updateReaderGroup operation.
     */
    public void updateReaderGroup(String scope, String rgName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_READER_GROUP), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_READER_GROUP, 1, readerGroupTags(scope, rgName));
        updateReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed updateReaderGroup operation.
     *
     * @param scope     Scope Name.
     * @param rgName    Reader Group Name.
     */
    public void updateReaderGroupFailed(String scope, String rgName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_READER_GROUP_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_READER_GROUP_FAILED, 1, readerGroupTags(scope, rgName));
    }

    /**
     * This method increments the global and ReaderGroup-specific counters for deleteReaderGroup operation
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param rgName        Name of the Reader Group.
     * @param latency       Latency of the deleteReaderGroup operation.
     */
    public void deleteReaderGroup(String scope, String rgName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_READER_GROUP), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_READER_GROUP, 1, readerGroupTags(scope, rgName));
        deleteReaderGroupLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed deleteReaderGroup operation attempts on a Reader Group.
     *
     * @param scope     Scope Name.
     * @param rgName    Reader Group Name.
     */
    public void deleteReaderGroupFailed(String scope, String rgName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_READER_GROUP_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_READER_GROUP_FAILED, 1, readerGroupTags(scope, rgName));
    }

    /**
     * This method increments the global and Stream-specific counters for updateSubscriberStreamCut operation on a Stream
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the updateSubscriberStreamCut operation.
     */
    public void updateTruncationSC(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_SUBSCRIBER), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_SUBSCRIBER, 1, streamTags(scope, streamName));
        updateSubscriberLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed updateSubscriberStreamCut operation attempts on a Stream.
     *
     * @param scope         Scope Name.
     * @param streamName    Stream Name.
     */
    public void updateTruncationSCFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_SUBSCRIBER_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_SUBSCRIBER_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream truncations and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the truncateStream operation.
     */
    public void truncateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(TRUNCATE_STREAM), 1);
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM, 1, streamTags(scope, streamName));
        truncateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream truncate operations in the system as well as the failed
     * truncate attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void truncateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(TRUNCATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the Stream-specific counter of retention operations.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public static void reportRetentionEvent(String scope, String streamName) {
        DYNAMIC_LOGGER.recordMeterEvents(RETENTION_FREQUENCY, 1, streamTags(scope, streamName));
    }

    /**
     * Reports the number of active segments for a Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param numSegments   Number of active segments in this Stream.
     */
    public static void reportActiveSegments(String scope, String streamName, int numSegments) {
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_COUNT, numSegments, streamTags(scope, streamName));
    }

    /**
     * Reports the number of segment splits and merges related to a particular scale operation on a Stream. Both global
     * and Stream-specific counters are updated.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param splits        Number of segment splits in the scale operation.
     * @param merges        Number of segment merges in the scale operation.
     */
    public static void reportSegmentSplitsAndMerges(String scope, String streamName, long splits, long merges) {
        DYNAMIC_LOGGER.reportGaugeValue(globalMetricName(SEGMENTS_SPLITS), splits);
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_SPLITS, splits, streamTags(scope, streamName));
        DYNAMIC_LOGGER.reportGaugeValue(globalMetricName(SEGMENTS_MERGES), merges);
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_MERGES, merges, streamTags(scope, streamName));
    }

    /**
     * Resets the OpsStatLogger objects to their initial state..
     */
    public static synchronized void reset() {
        StreamMetrics old = INSTANCE.get();
        if (old != null) {
            old.createStreamLatency.close();
            old.deleteStreamLatency.close();
            old.sealStreamLatency.close();
            old.updateStreamLatency.close();
            old.truncateStreamLatency.close();
            old.addReaderGroupLatency.close();
            old.deleteReaderGroupLatency.close();
            old.updateSubscriberLatency.close();
            old.createScopeLatency.close();
            old.deleteScopeLatency.close();
            old.controllerEventProcessorDeleteStreamLatency.close();
            old.controllerEventProcessorSealStreamLatency.close();
            old.controllerEventProcessorTruncateStreamLatency.close();
            old.controllerEventProcessorUpdateStreamLatency.close();
            old.controllerEventProcessorAutoScaleStreamLatency.close();
            old.controllerEventProcessorCreateReaderGroupLatency.close();
            old.controllerEventProcessorDeleteReaderGroupLatency.close();
            old.controllerEventProcessorUpdateReaderGroupLatency.close();
            old.controllerEventProcessorDeleteScopeLatency.close();
            old.controllerEventProcessorScaleStreamLatency.close();
            old.controllerEventProcessorCreateTableLatency.close();
            old.controllerEventProcessorDeleteTableLatency.close();
        }
        INSTANCE.set(new StreamMetrics());
    }
}
