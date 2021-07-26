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
package io.pravega.shared.health;

import io.pravega.shared.health.impl.AbstractHealthContributor;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
public class TestHealthContributors {

    /**
     * Implements a {@link HealthContributor} that always supplies a healthy result. This class also sets one
     * details entry.
     */
    public static class HealthyContributor extends AbstractHealthContributor {
        public static final String DETAILS_KEY = "indicator-details-key";

        public static final String DETAILS_VAL = "sample-indicator-details-value";

        public HealthyContributor() {
            super("healthy", StatusAggregator.UNANIMOUS);
        }

        public HealthyContributor(String name) {
            super(name, StatusAggregator.UNANIMOUS);
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }

    /**
     * Implements an {@link HealthContributor} that *always* supplies a 'failing' result.
     */
    public static class FailingContributor extends AbstractHealthContributor {
        public FailingContributor(String name) {
            super(name, StatusAggregator.UNANIMOUS);
        }

        public FailingContributor() {
            this("failing");
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.DOWN;
            builder.status(status);
            return status;
        }
    }


    /**
     * Implements an {@link HealthContributor} that *always* will throw an error within it's {@link AbstractHealthContributor#doHealthCheck(Health.HealthBuilder)}.
     */
    public static class ThrowingContributor extends AbstractHealthContributor {
        public ThrowingContributor() {
            super("thrower");
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UNKNOWN;
            throw new RuntimeException();
        }
    }

    public static void awaitHealthContributor(HealthServiceManager service, String id) throws TimeoutException {
        TestUtils.await(() -> {
                    Health health = null;
                    try {
                        health = service.getEndpoint().getHealth(id);
                    } catch (ContributorNotFoundException e) {
                        log.info("ContributorNotFound: '{}', retrying ...", id);
                    }
                    return health != null && health.getStatus() != Status.UNKNOWN;
                }, (int) service.getHealthServiceUpdater().getInterval().toMillis(),
                (int) service.getHealthServiceUpdater().getInterval().toMillis() * 3);
    }

}
