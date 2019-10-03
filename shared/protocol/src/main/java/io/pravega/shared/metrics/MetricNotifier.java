/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

public interface MetricNotifier extends AutoCloseable {

    MetricNotifier NO_OP_METRIC_NOTIFIER = new MetricNotifier() {
        @Override
        public void updateSuccessMetric(String metricName, long value) {
        }

        @Override
        public void updateFailureMetric(String metricName, long value) {
        }

        @Override
        public void close() {
        }
    };

    void updateSuccessMetric(String metricName, long value);

    void updateFailureMetric(String metricName, long value);

    @Override
    void close();
}
