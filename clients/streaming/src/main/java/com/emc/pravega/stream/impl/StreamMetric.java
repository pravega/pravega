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
package com.emc.pravega.stream.impl;

import lombok.Data;

import java.time.Duration;
import java.util.List;

@Data
public class StreamMetric implements Metric {

    public enum RateType {
        RATE_IN_BYTES,
        RATE_IN_EVENTS,
    }

    @Data
    public static class SegmentId {
        final String stream;
        final String scope;
        final int number;
    }

    final SegmentId segmentId;
    final long timestamp;
    final Duration period;
    final RateType rateType;
    final long avgRate;
    final List<Double> chunks;

    @Override
    public MetricType getType() {
        return MetricType.StreamMetric;
    }
}