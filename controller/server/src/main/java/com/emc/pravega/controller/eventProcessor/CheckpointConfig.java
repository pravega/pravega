/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for event processor's position object persistence configuration.
 */
@Data
@Builder
public class CheckpointConfig {
    public enum Type {
        None,
        Periodic
    }

    public enum StoreType {
        InMemory,
        Zookeeper,
    }

    @Data
    @Builder
    public static class CheckpointPeriod {
        private final int numEvents;
        private final int numSeconds;

        private CheckpointPeriod(int numEvents, int numSeconds) {
            Preconditions.checkArgument(numEvents > 0, "numEvents should be positive integer");
            Preconditions.checkArgument(numSeconds > 0, "numSeconds should be positive integer");
            this.numEvents = numEvents;
            this.numSeconds = numSeconds;
        }
    }

    private final Type type;
    private final StoreType storeType;
    private final CheckpointPeriod checkpointPeriod;
    private final Object checkpointStoreClient;
}
