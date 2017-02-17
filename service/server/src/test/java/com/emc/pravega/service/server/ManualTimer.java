/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.common.AbstractTimer;
import lombok.Getter;

/**
 * AbstractTimer Implementation that allows full control over the return values, for use in Unit testing.
 */
public class ManualTimer extends AbstractTimer {
    @Getter
    private long elapsedNanos;

    public void setElapsedMillis(long value) {
        this.elapsedNanos = value * NANOS_TO_MILLIS;
    }
}
