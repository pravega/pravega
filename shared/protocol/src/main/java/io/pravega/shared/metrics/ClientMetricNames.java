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
import com.google.common.base.Strings;

public final class ClientMetricNames {

    private static final String PREFIX = "pravega" + ".";

    // Metrics for client
    public static final String CLIENT_APPEND_LATENCY = PREFIX + "client.segment.append_latency_ms";

    public static String globalMetricName(String stringName) {
        return stringName + "_global";
    }

    /**
     * Create an MetricKey object based on metric name, metric type and tags associated.
     * The MetricKey object contains cache key for cache lookup and registry key for registry lookup.
     *
     * @param metric the metric name.
     * @param tags the tag(s) associated with the metric.
     * @return the MetricKey object contains cache lookup key and metric registry key.
     */
    public static String metricKey(String metric, String... tags) {

        if (tags == null || tags.length == 0) {  //if no tags supplied, the original metric name is used for both cache key and registry key.
            return metric;
        } else { //if tag is supplied, append tag value to form cache key; original metric name is registry key.
            StringBuilder sb = new StringBuilder(metric);
            Preconditions.checkArgument((tags.length % 2) == 0, "Tags is a set of key/value pair so the size must be even: %s", tags.length);
            for (int i = 0; i < tags.length; i += 2) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(tags[i]) || !Strings.isNullOrEmpty(tags[i + 1]), "Tag name or value cannot be empty or null");
                sb.append('.').append(tags[i + 1]);
            }
            return sb.toString();
        }
    }
}
