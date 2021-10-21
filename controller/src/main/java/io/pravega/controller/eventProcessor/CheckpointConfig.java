/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.eventProcessor;

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
        Preconditions.checkArgument(
                (type == Type.None && checkpointPeriod == null) || (type == Type.Periodic && checkpointPeriod != null),
                "CheckpointPeriod should be non-null if Type is not None");

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
