/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.fault.impl;

import io.pravega.server.controller.service.fault.ControllerClusterListenerConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

/**
 * Controller cluster configuration.
 */
@Getter
public class ControllerClusterListenerConfigImpl implements ControllerClusterListenerConfig {
    private final int minThreads;
    private final int maxThreads;
    private final int idleTime;
    private final TimeUnit idleTimeUnit;
    private final int maxQueueSize;

    @Builder
    ControllerClusterListenerConfigImpl(final int minThreads, final int maxThreads, final int idleTime,
                                        final TimeUnit idleTimeUnit,
                                        final int maxQueueSize) {
        Preconditions.checkArgument(minThreads > 0, "minThreads should be positive integer");
        Preconditions.checkArgument(maxThreads >= minThreads, "maxThreads should be >= minThreads");
        Preconditions.checkArgument(idleTime > 0, "idleTime should be positive integer");
        Preconditions.checkNotNull(idleTimeUnit, "idleTimeUnit");
        Preconditions.checkArgument(maxQueueSize > 0 && maxQueueSize <= 1024,
                "maxQueueSize should be positive integer smaller than 1024");

        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.idleTime = idleTime;
        this.idleTimeUnit = idleTimeUnit;
        this.maxQueueSize = maxQueueSize;
    }
}
