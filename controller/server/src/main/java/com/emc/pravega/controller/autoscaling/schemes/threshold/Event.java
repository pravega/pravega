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
package com.emc.pravega.controller.autoscaling.schemes.threshold;

/**
 * Event interface.
 */
public interface Event {
    /**
     * Segment for which the event was produced.
     *
     * @return Segment number.
     */
    Integer getSegmentNumber();

    /**
     * Metric's timestamp against which event was produced.
     * Note: This is not the time when event was generated on controller
     * but the time when metric was produced at the source.
     *
     * @return Event time.
     */
    long getEventTime();

    /**
     * Metric's rate for sampling period.
     *
     * @return Metric Rate.
     */
    long getMetricAvgValue();

    long getEventPeriod();
}
