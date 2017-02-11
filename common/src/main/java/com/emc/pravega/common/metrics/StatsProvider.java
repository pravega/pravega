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

/**
 * Provider of StatsLogger instances depending on scope.
 * An implementation of this interface possibly returns a separate instance per Pravega scope.
 */
public interface StatsProvider extends AutoCloseable {
    /**
     * Initialize the stats provider by loading the given configuration <i>conf</i>.
     *
     * @param conf Configuration to configure the stats provider.
     */
    void start(MetricsConfig conf);

    /**
     * Close the stats provider.
     */
    @Override
    void close();

    /**
     * Return the StatsLogger instance associated with the given <i>scope</i>.
     *
     * @param scope Scope for the given stats.
     * @return stats logger for the given <i>scope</i>.
     */
    StatsLogger createStatsLogger(String scope);

    /**
     * Create a dynamic logger.
     *
     * @return logger instance.
     */
    DynamicLogger createDynamicLogger();
}
