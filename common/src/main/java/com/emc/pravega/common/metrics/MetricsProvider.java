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
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricsProvider {
    public static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final StatsProvider NULLPROVIDER = new NullStatsProvider();
    private static final StatsProvider YAMMERPROVIDER = new YammerStatsProvider();
    private static final MetricsProvider INSTANCE  = new MetricsProvider();

    // Dynamic logger
    private static final DynamicLogger YAMMERDYNAMICLOGGER = YAMMERPROVIDER.createDynamicLogger();
    private static final DynamicLogger NULLDYNAMICLOGGER = NULLPROVIDER.createDynamicLogger();

    private MetricsProvider() {
    }

    public static StatsProvider getMetricsProvider() {
        return  MetricsConfig.enableStatistics() ? YAMMERPROVIDER : NULLPROVIDER;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        return  MetricsConfig.enableStatistics() ?
                YAMMERPROVIDER.createStatsLogger(loggerName) :
                NULLPROVIDER.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        return  MetricsConfig.enableStatistics() ?
                YAMMERDYNAMICLOGGER :
                NULLDYNAMICLOGGER;
    }
}
