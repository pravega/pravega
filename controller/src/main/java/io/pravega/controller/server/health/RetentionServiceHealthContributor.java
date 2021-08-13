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
package io.pravega.controller.server.health;

import com.google.common.base.Preconditions;
import io.pravega.controller.server.bucket.BucketManager;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetentionServiceHealthContributor extends AbstractHealthContributor {
    private final BucketManager retentionService;

    public RetentionServiceHealthContributor(String name, BucketManager retentionService) {
        super(name);
        this.retentionService = Preconditions.checkNotNull(retentionService, "retentionService");
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        Status status = Status.DOWN;
        if (retentionService.isRunning()) {
            status = Status.NEW;
            if (retentionService.isHealthy()) {
                status = Status.UP;
            }
        }
        return status;
    }
}

