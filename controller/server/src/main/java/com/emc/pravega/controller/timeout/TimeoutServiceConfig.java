/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.timeout;

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
        Preconditions.checkArgument(maxLeaseValue > 0);
        Preconditions.checkArgument(maxScaleGracePeriod > 0);

        this.maxLeaseValue = maxLeaseValue;
        this.maxScaleGracePeriod = maxScaleGracePeriod;
    }

    public static TimeoutServiceConfig defaultConfig() {
        return new TimeoutServiceConfig(30000, 30000);
    }
}
