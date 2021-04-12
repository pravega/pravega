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
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import java.time.Duration;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;

import static io.pravega.shared.MetricsNames.failMetricName;

@EqualsAndHashCode
class OpStatsLoggerImpl implements OpStatsLogger {
    //region Members

    private final Timer success;
    private final Timer fail;
    private final MeterRegistry meterRegistry;

    //endregion

    //region Constructor

    OpStatsLoggerImpl(MeterRegistry metricRegistry, String statName, String... tags) {
        this.meterRegistry = Preconditions.checkNotNull(metricRegistry, "metrics");
        //This will publish additional percentile metrics
        this.success = Timer.builder(statName).tags(tags).publishPercentiles(OpStatsData.PERCENTILE_ARRAY)
                .register(this.meterRegistry);
        this.fail = Timer.builder(failMetricName(statName)).tags(tags).publishPercentiles(OpStatsData.PERCENTILE_ARRAY)
                .register(this.meterRegistry);
    }

    //endregion

    //region AutoCloseable and Finalizer Implementation

    @Override
    public void close() {
        this.meterRegistry.remove(success);
        this.meterRegistry.remove(fail);
    }


    //endregion

    //region OpStatsLogger Implementation

    @Override
    public Meter.Id getId() {
        return this.success.getId();
    }

    @Override
    public void reportFailEvent(Duration duration) {
        fail.record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        success.record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        success.record(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportFailValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        fail.record(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void clear() {
        // can't clear a timer
    }

    @Override
    public OpStatsData toOpStatsData() {
        long numFailed = fail.count();
        long numSuccess = success.count();
        HistogramSnapshot snapshot = success.takeSnapshot();
        double avgLatencyMillis = snapshot.mean();

        EnumMap<OpStatsData.Percentile, Long> percentileLongMap  =
                new EnumMap<>(OpStatsData.Percentile.class);

        //Only add entries into percentile map when percentile values are not missing from snapshot.
        if (OpStatsData.PERCENTILE_ARRAY.length == snapshot.percentileValues().length) {
            int index = 0;
            for (OpStatsData.Percentile percent : OpStatsData.PERCENTILE_SET) {
                percentileLongMap.put(percent, (long) snapshot.percentileValues()[index++].value());
            }
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, percentileLongMap);
    }

    //endregion
}
