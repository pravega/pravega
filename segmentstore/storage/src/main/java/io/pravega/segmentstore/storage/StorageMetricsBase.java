/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.OpStatsLogger;
import lombok.Getter;

public class StorageMetricsBase {
    @Getter
    protected OpStatsLogger readLatency;
    @Getter
    protected OpStatsLogger writeLatency;
    @Getter
    protected Counter readBytes;
    @Getter
    protected Counter writeBytes;
}
