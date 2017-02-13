/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private final CheckpointStore.StoreType storeType;
    private final CheckpointPeriod checkpointPeriod;
    private final Object checkpointStoreClient;
}
