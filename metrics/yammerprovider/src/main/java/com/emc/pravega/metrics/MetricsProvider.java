/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricsProvider {
    private static MetricsProvider instance  = new MetricsProvider();

    private static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final StatsProvider NULLPROVIDER = new NullStatsProvider();
    private static final StatsProvider YAMMERPROVIDER = new YammerStatsProvider();

    private MetricsProvider() {
    }

    public static MetricRegistry getMetrics() {
        return YAMMERMETRICS;
    }

    public static StatsProvider getProvider() {
        return YAMMERPROVIDER;
    }

    public static StatsProvider getNullProvider() {
        return NULLPROVIDER;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        return YAMMERPROVIDER.createStatsLogger(loggerName);
    }
}
