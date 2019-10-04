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

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.MultiKeyLatestItemSequentialProcessor;
import java.util.concurrent.ScheduledExecutorService;


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
    public void updateSuccessMetric(String metric, String[] metricTags, long value) {
        successProcessor.updateItem(ClientMetricNames.metricKey(metric, metricTags), value);
    }

    @Override
    public void updateFailureMetric(String metricKey, String[] metric,  long value) {
        failureProcessor.updateItem(ClientMetricNames.metricKey(metricKey, metric), value);
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(clientMetricExecutor);
    }
}
