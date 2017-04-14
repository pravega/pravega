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
public class CheckpointConfig {
    public enum Type {
        None,
        Periodic
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
    private final CheckpointPeriod checkpointPeriod;

    @Builder
    CheckpointConfig(final Type type, final CheckpointPeriod checkpointPeriod) {
        Preconditions.checkNotNull(type);
        Preconditions.checkArgument((type.equals(Type.None) && checkpointPeriod == null) ||
                (type.equals(Type.Periodic) && checkpointPeriod != null));
        this.type = type;
        this.checkpointPeriod = checkpointPeriod;
    }

    public static CheckpointConfig periodic(final int numEvents, final int numSeconds) {
        return new CheckpointConfig(Type.Periodic, new CheckpointPeriod(numEvents, numSeconds));
    }

    public static CheckpointConfig none() {
        return new CheckpointConfig(Type.None, null);
    }

}
