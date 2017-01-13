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
package com.emc.pravega.controller.monitoring.schemes.threshold;

public class Events {

    public static class HighThreshold implements Event {
        private final int segmentNumber;
        private final long eventTime;
        private final long metricAvgValue;
        private final long eventPeriod;

        public HighThreshold(int segmentNumber, long eventTime, long metricAvgValue, long eventPeriod) {
            this.segmentNumber = segmentNumber;
            this.eventTime = eventTime;
            this.metricAvgValue = metricAvgValue;
            this.eventPeriod = eventPeriod;
        }

        @Override
        public Integer getSegmentNumber() {
            return segmentNumber;
        }

        @Override
        public long getEventTime() {
            return eventTime;
        }

        @Override
        public long getMetricAvgValue() {
            return metricAvgValue;
        }

        @Override
        public long getEventPeriod() {
            return eventPeriod;
        }
    }

    public static class LowThreshold implements Event {
        private final int segmentNumber;
        private final long eventTime;
        private final long metricAvgValue;
        private final long eventPeriod;

        public LowThreshold(int segmentNumber, long eventTime, long metricAvgValue, long eventPeriod) {
            this.segmentNumber = segmentNumber;
            this.eventTime = eventTime;
            this.metricAvgValue = metricAvgValue;
            this.eventPeriod = eventPeriod;
        }

        @Override
        public Integer getSegmentNumber() {
            return segmentNumber;
        }

        @Override
        public long getEventTime() {
            return eventTime;
        }

        @Override
        public long getMetricAvgValue() {
            return metricAvgValue;
        }

        @Override
        public long getEventPeriod() {
            return eventPeriod;
        }
    }
}
