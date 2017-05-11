/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.timeout;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

/**
 * Timeout service config.
 */
@Getter
public class TimeoutServiceConfig {
    private final long maxLeaseValue;
    private final long maxScaleGracePeriod;

    @Builder
    TimeoutServiceConfig(final long maxLeaseValue, final long maxScaleGracePeriod) {
        Preconditions.checkArgument(maxLeaseValue > 0, "maxLeaseValue should be positive integer");
        Preconditions.checkArgument(maxScaleGracePeriod > 0, "maxScaleGracePeriod should be positive integer");

        this.maxLeaseValue = maxLeaseValue;
        this.maxScaleGracePeriod = maxScaleGracePeriod;
    }

    public static TimeoutServiceConfig defaultConfig() {
        return new TimeoutServiceConfig(30000, 30000);
    }
}
