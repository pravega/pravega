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
package com.emc.pravega.controller.autoscaling;

import com.emc.pravega.controller.autoscaling.util.BackgroundWorker;
import com.emc.pravega.stream.impl.Metric;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Base class for Stream and host Monitors. It implements a method to receive a metric and put it in a queue.
 *
 * @param <T> Type of value to be put in the queue
 */
public abstract class MonitorWorker<T extends Metric> extends BackgroundWorker<T> {
    /**
     * Static executor shared by all monitors across streams and hosts.
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(100);
    private final ConcurrentLinkedQueue<T> queue;

    public MonitorWorker() {
        super(EXECUTOR);
        queue = new ConcurrentLinkedQueue<>();
    }

    public void incoming(T metric) {
        queue.add(metric);
    }

    @Override
    public T getNextWork() {
        return queue.poll();
    }
}
