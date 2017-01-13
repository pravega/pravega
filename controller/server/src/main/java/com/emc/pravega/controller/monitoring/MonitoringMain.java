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
package com.emc.pravega.controller.monitoring;

import com.emc.pravega.common.metric.Metric;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is responsible for reading metrics from an external source.
 * It is also an observer for hostSet and streamSet and listens for any changes
 * to hosts or streams. It calls into appropriate methods of autoscaler corresponding to the signal received.
 * The signals could be about new host added, new stream created, any scaled event done, any policy updated.
 */
@Data
public class MonitoringMain implements Runnable {
    private final MetricManager metricManager;

    public MonitoringMain(final MetricManager metricManager) {
        this.metricManager = metricManager;
    }

    @Override
    public void run() {
        // TODO: read metrics. Once we finalize on how and where to read metrics from, this component can be activated.
        while (true) {
            List<Metric> metrics = new ArrayList<>();
            metricManager.handleIncomingMetrics(metrics);
        }
    }
}
