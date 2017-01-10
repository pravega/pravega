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

import com.emc.pravega.controller.monitoring.history.AggregatedValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate value where we store moving average rate per segment.
 */
public class MovingRateValue implements AggregatedValue<Map<Integer, Long>> {

    /**
     * Map of segment number to segment's approximate average rate over rolling window.
     */
    private final Map<Integer, Long> rollingAverageRate;

    public MovingRateValue() {
        this.rollingAverageRate = new HashMap<>();
    }

    @Override
    public Map<Integer, Long> getValue() {
        return rollingAverageRate;
    }

    public void update(int segmentNumber, long newAverage) {
        rollingAverageRate.put(segmentNumber, newAverage);
    }
}
