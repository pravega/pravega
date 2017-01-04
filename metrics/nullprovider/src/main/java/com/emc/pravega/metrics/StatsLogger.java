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
    public OpStatsLogger getOpStatsLogger(String name);

    /**
     * Gets counter.
     *
     * @param name Stats Name
     * @return Get the logger for a simple stat described by the <i>name</i>
     */
    public Counter getCounter(String name);

    /**
     * Register given <i>guage</i> as name <i>name</i>.
     *
     * @param <T>   the type parameter
     * @param name  gauge name
     * @param gauge the gauge
     */
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge);

    /**
     * Register gauge.
     *
     * @param name  gauge name
     * @param value the gauge value
     */
    public void registerGauge(String name, final Long value);

    /**
     * Provide the stats logger under scope <i>name</i>.
     *
     * @param name scope name.
     * @return stats logger under scope <i>name</i>.
     */
    public StatsLogger scope(String name);

}
