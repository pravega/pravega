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
package io.pravega.segmentstore.server.logs.health;

import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.server.logs.InMemoryLog;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;

public class InMemoryLogHealthContributor extends AbstractHealthContributor {

    private final InMemoryLog log;

    public InMemoryLogHealthContributor(@NonNull String name, InMemoryLog log) {
        super(name);
        this.log = log;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        builder.details(ImmutableMap.of("Size", log.size()));
        return Status.RUNNING;
    }
}
