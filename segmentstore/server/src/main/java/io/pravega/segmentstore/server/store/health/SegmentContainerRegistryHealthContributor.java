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
package io.pravega.segmentstore.server.store.health;

import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;

public class SegmentContainerRegistryHealthContributor extends AbstractHealthContributor {
    private final SegmentContainerRegistry segmentContainerRegistry;

    public SegmentContainerRegistryHealthContributor(@NonNull SegmentContainerRegistry segmentContainerRegistry) {
        super("SegmentContainerRegistry");
        this.segmentContainerRegistry = segmentContainerRegistry;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {

        Status status = Status.TERMINATED;
        boolean ready = !segmentContainerRegistry.isClosed();

        if (ready) {
            status = Status.RUNNING;
        }

        return status;
    }
}