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
package io.pravega.segmentstore.server.host.stat;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Implementation for {@link TableSegmentStatsRecorder}.
 */
@Getter(AccessLevel.PACKAGE)
class TableSegmentStatsRecorderImpl implements TableSegmentStatsRecorder {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private final OpStatsLogger createSegment = createLogger(MetricsNames.SEGMENT_CREATE_LATENCY);
    private final OpStatsLogger deleteSegment = createLogger(MetricsNames.SEGMENT_DELETE_LATENCY);
    private final OpStatsLogger updateConditionalLatency = createLogger(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL_LATENCY);
    private final Counter updateConditional = createCounter(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL);
    private final OpStatsLogger updateUnconditionalLatency = createLogger(MetricsNames.TABLE_SEGMENT_UPDATE_LATENCY);
    private final Counter updateUnconditional = createCounter(MetricsNames.TABLE_SEGMENT_UPDATE);
    private final OpStatsLogger removeConditionalLatency = createLogger(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL_LATENCY);
    private final Counter removeConditional = createCounter(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL);
    private final OpStatsLogger removeUnconditionalLatency = createLogger(MetricsNames.TABLE_SEGMENT_REMOVE_LATENCY);
    private final Counter removeUnconditional = createCounter(MetricsNames.TABLE_SEGMENT_REMOVE);
    private final OpStatsLogger getKeysLatency = createLogger(MetricsNames.TABLE_SEGMENT_GET_LATENCY);
    private final Counter getKeys = createCounter(MetricsNames.TABLE_SEGMENT_GET);
    private final OpStatsLogger iterateKeysLatency = createLogger(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS_LATENCY);
    private final Counter iterateKeys = createCounter(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS);
    private final OpStatsLogger iterateEntriesLatency = createLogger(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES_LATENCY);
    private final Counter iterateEntries = createCounter(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES);
    private final OpStatsLogger getInfoLatency = createLogger(MetricsNames.TABLE_SEGMENT_GET_INFO_LATENCY);
    private final Counter getInfo = createCounter(MetricsNames.TABLE_SEGMENT_GET_INFO);

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.createSegment.close();
        this.deleteSegment.close();
        this.updateConditionalLatency.close();
        this.updateConditional.close();
        this.updateUnconditionalLatency.close();
        this.updateUnconditional.close();
        this.removeConditionalLatency.close();
        this.removeConditional.close();
        this.removeUnconditionalLatency.close();
        this.removeUnconditional.close();
        this.getKeysLatency.close();
        this.getKeys.close();
        this.iterateKeysLatency.close();
        this.iterateKeys.close();
        this.iterateEntriesLatency.close();
        this.iterateEntries.close();
        this.getInfo.close();
        this.getInfoLatency.close();
    }

    //endregion

    //region TableSegmentStatsRecorder Implementation

    @Override
    public void createTableSegment(String tableSegmentName, Duration elapsed) {
        this.createSegment.reportSuccessEvent(elapsed);
    }

    @Override
    public void deleteTableSegment(String tableSegmentName, Duration elapsed) {
        this.deleteSegment.reportSuccessEvent(elapsed);
    }

    @Override
    public void updateEntries(String tableSegmentName, int entryCount, boolean conditional, Duration elapsed) {
        choose(conditional, this.updateConditionalLatency, this.updateUnconditionalLatency).reportSuccessEvent(elapsed);
        choose(conditional, this.updateConditional, this.updateUnconditional).add(entryCount);
    }

    @Override
    public void removeKeys(String tableSegmentName, int keyCount, boolean conditional, Duration elapsed) {
        choose(conditional, this.removeConditionalLatency, this.removeUnconditionalLatency).reportSuccessEvent(elapsed);
        choose(conditional, this.removeConditional, this.removeUnconditional).add(keyCount);
    }

    @Override
    public void getKeys(String tableSegmentName, int keyCount, Duration elapsed) {
        this.getKeysLatency.reportSuccessEvent(elapsed);
        this.getKeys.add(keyCount);
    }

    @Override
    public void iterateKeys(String tableSegmentName, int resultCount, Duration elapsed) {
        this.iterateKeysLatency.reportSuccessEvent(elapsed);
        this.iterateKeys.add(resultCount);
    }

    @Override
    public void iterateEntries(String tableSegmentName, int resultCount, Duration elapsed) {
        this.iterateEntriesLatency.reportSuccessEvent(elapsed);
        this.iterateEntries.add(resultCount);
    }

    @Override
    public void getInfo(String tableSegmentName, Duration elapsed) {
        this.getInfoLatency.reportSuccessEvent(elapsed);
        this.getInfo.inc();
    }

    //endregion

    protected OpStatsLogger createLogger(String name) {
        return STATS_LOGGER.createStats(name);
    }

    protected Counter createCounter(String name) {
        return STATS_LOGGER.createCounter(name);
    }

    private <T> T choose(boolean conditional, T whenConditional, T whenUnconditional) {
        return conditional ? whenConditional : whenUnconditional;
    }
}
