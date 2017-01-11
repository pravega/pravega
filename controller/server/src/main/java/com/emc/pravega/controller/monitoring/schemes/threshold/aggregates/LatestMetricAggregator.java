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
package com.emc.pravega.controller.monitoring.schemes.threshold.aggregates;

import com.emc.pravega.controller.monitoring.InjectableBehaviours;
import com.emc.pravega.controller.monitoring.history.History;
import com.emc.pravega.common.metric.Metric;
import lombok.Data;

/**
 * Behaviour injected that tells history how to compute aggregate upon receiving a metric sample.
 */
@Data
public class LatestMetricAggregator<M extends Metric, S, H extends History<M, S>>
        implements InjectableBehaviours.AggregateFunction<LastMetricValue<M>, S, M, H> {

    /**
     * This method stores last metric.
     *
     * @param metric   Incoming metric
     * @param previous Previous aggregate value
     * @param history  History
     * @return new aggregate value
     */
    @Override
    public LastMetricValue<M> compute(M metric, LastMetricValue<M> previous, H history) {
        previous.update(metric);
        return previous;
    }
}
