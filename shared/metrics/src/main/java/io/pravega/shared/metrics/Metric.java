/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.shared.metrics;

import io.micrometer.core.instrument.Meter.Id;

/**
 * Defines common methods for a Metric.
 */
interface Metric extends AutoCloseable {
    /**
     * Gets Id of metric.
     *
     * @return the id of metric.
     */
    Id getId();

    @Override
    void close();
}
