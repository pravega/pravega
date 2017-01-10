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

import com.emc.pravega.controller.monitoring.FunctionalInterfaces;
import com.emc.pravega.controller.monitoring.schemes.threshold.Event;
import com.emc.pravega.controller.monitoring.schemes.threshold.history.EventHistory;
import com.emc.pravega.stream.impl.StreamMetric;

/**
 * Behaviour injected that tells history how to compute aggregate upon receiving a metric sample.
 */
public class SegmentAggregator implements FunctionalInterfaces.AggregateFunction<SegmentQuantileValue, Event, StreamMetric, EventHistory> {

    /**
     * Function to update SegmentQuantileValue.
     *
     * @param metric   Incoming metric
     * @param previous Previous aggregate value
     * @param history  History
     * @return
     */
    @Override
    public SegmentQuantileValue compute(final StreamMetric metric, final SegmentQuantileValue previous, final EventHistory history) {
        previous.update(metric);
        return previous;
    }
}
