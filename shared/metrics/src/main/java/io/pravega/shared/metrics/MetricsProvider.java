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

public class MetricsProvider {

    private static final StatsProviderProxy STATS_PROVIDER = new StatsProviderProxy();
    private static final DynamicLoggerProxy DYNAMIC_LOGGER = new DynamicLoggerProxy(STATS_PROVIDER.createDynamicLogger());

    public synchronized static void initialize(MetricsConfig config) {
        STATS_PROVIDER.setProvider(config);
        DYNAMIC_LOGGER.setLogger(STATS_PROVIDER.createDynamicLogger());
    }

    public synchronized static StatsProvider getMetricsProvider() {
        return STATS_PROVIDER;
    }

    public synchronized static StatsLogger createStatsLogger(String loggerName) {
        return STATS_PROVIDER.createStatsLogger(loggerName);
    }

    public synchronized static DynamicLogger getDynamicLogger() {
        return DYNAMIC_LOGGER;
    }
}
