/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

/**
 * A meter metric which measures mean throughput and exponentially-weighted moving average throughput.
 */
public interface Meter extends Metric {
    /**
     * Record the occurrence of an event in Meter.
     */
    void recordEvent();

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param n the number of events to mark
     */
    void recordEvents(long n);

    /**
     * Returns the number of events which have been marked.
     *
     * @return the count
     */
    long getCount();
}
