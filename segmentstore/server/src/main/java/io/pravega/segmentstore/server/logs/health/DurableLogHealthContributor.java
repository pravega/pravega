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
import io.pravega.segmentstore.server.logs.DurableLog;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.contributors.ServiceHealthContributor;

public class DurableLogHealthContributor extends ServiceHealthContributor {

    private final DurableLog log;

    public DurableLogHealthContributor(String name, DurableLog log) {
        super(name, log);
        this.log = log;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        Status status = super.doHealthCheck(builder);

        if (log.isOffline()) {
            status = Status.TERMINATED;
        }

        builder.details(ImmutableMap.of("State", log.state(), "IsOffline", log.isOffline()));

        return status;
    }

}
