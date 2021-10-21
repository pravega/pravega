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

/**
 * A notifier that is used to notify metrics to the user specified {@link MetricListener}.
 *
 */
public interface MetricNotifier extends AutoCloseable {

    MetricNotifier NO_OP_METRIC_NOTIFIER = new MetricNotifier() {
        @Override
        public void updateSuccessMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
        }

        @Override
        public void updateFailureMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
        }

        @Override
        public void close() {
        }
    };

    /**
     * Notify a success metric to the user specified {@link MetricListener}.
     * @param metricKey The metric key.
     * @param metricTags Tags associated with the metric.
     * @param value Value of the metric observed.
     */
    void updateSuccessMetric(ClientMetricKeys metricKey, String[] metricTags, long value);

    /**
     * Notify a failure metric to the user specified {@link MetricListener}.
     * @param metricKey The metric key.
     * @param metricTags Tags associated with the metric.
     * @param value Value of the metric observed.
     */
    void updateFailureMetric(ClientMetricKeys metricKey, String[] metricTags,  long value);

    @Override
    void close();
}
