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

import java.util.function.Supplier;

/**
 * A simple interface that exposes just 2 kind of methods. One to get the logger for an Op stat
 * and another to get the logger for a simple stat(Counter and Gauge)
 */
public interface StatsLogger {
    /**
     * Gets op stats logger.
     *
     * @param name Stats Name
     * @return Get the logger for an OpStat described by the <i>name</i>.
     */
    public OpStatsLogger createStats(String name);

    /**
     * Gets counter.
     *
     * @param name Stats Name
     * @return Get the logger for a simple stat described by the <i>name</i>
     */
    public Counter createCounter(String name);

    /**
     * Register gauge.
     * <i>value</i> is usually get of Number: AtomicInteger::get, AtomicLong::get
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the supplier to provide value through get()
     */
    public <T extends Number> void registerGauge(String name, Supplier<T> value);

    /**
     * Provide the stats logger under scope <i>scope</i>.
     *
     * @param scope scope name.
     * @return stats logger under scope <i>scope</i>.
     */
    public StatsLogger createScopeLogger(String scope);

}
