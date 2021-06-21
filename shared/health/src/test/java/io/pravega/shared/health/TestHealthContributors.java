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

import java.util.HashMap;
import java.util.Map;

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

        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UNKNOWN;
            throw new RuntimeException();
        }
    }

}
