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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.MultiKeyLatestItemSequentialProcessor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A MetricNotifier that invokes the {@link MetricListener} provided by the user. This notifier
 * skips updates if multiple updates are performed while the MetricListener is processing, only the most
 * recent update is used.
 */
public class ClientMetricUpdater implements MetricNotifier {
    private final MultiKeyLatestItemSequentialProcessor<String, Long> successProcessor;
    private final MultiKeyLatestItemSequentialProcessor<String, Long> failureProcessor;

    private final ScheduledExecutorService clientMetricExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "clientMetrics");

    public ClientMetricUpdater(final MetricListener metricListener) {
        Preconditions.checkNotNull(metricListener);
        this.successProcessor = new MultiKeyLatestItemSequentialProcessor<>(metricListener::reportSuccessValue, clientMetricExecutor);
        this.failureProcessor = new MultiKeyLatestItemSequentialProcessor<>(metricListener::reportFailValue, clientMetricExecutor);
    }

    @Override
    public void updateSuccessMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
        successProcessor.updateItem(metricKey.metric(metricTags), value);
    }

    @Override
    public void updateFailureMetric(ClientMetricKeys metricKey, String[] metric,  long value) {
        failureProcessor.updateItem(metricKey.metric(metric), value);
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(clientMetricExecutor);
    }
}
