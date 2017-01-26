/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.metrics;

import java.util.function.Supplier;

/**
 * A simple interface that only exposes simple type metrics: Counter/Gauge.
 */
public interface DynamicLogger {

    /**
     * Create counter counter.
     *
     * @param name Counter Name
     * @return Create and register counter described by the <i>name</i>
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
    public <T extends Number> Gauge registerGauge(final String name, Supplier<T> value);
}