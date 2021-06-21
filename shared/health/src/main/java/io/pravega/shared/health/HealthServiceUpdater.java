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

import com.google.common.util.concurrent.Service;

import java.time.Duration;

/**
 * The {@link HealthServiceUpdater} is responsible for regularly updating the {@link Health} of the {@link HealthServiceManager}.
 * This is useful in cases where health information is not regularly queried by some client. In the event of a crash or failure,
 * (should we want to provide this information) it allows us to place an upper bound on how stale this {@link Health}
 * information may be.
 */
public interface HealthServiceUpdater extends Service, AutoCloseable {

    /**
     * Supplies the most recent {@link Health} check result.
     *
     * @return The {@link Health} of the last health check.
     */
    Health getLatestHealth();

    /**
     * The interval (in seconds) at which the {@link HealthServiceUpdater} performs the health checks in.
     * @return The interval in which the executor will call {@link HealthEndpoint#getHealth()}.
     */
    Duration getInterval();

    @Override
    void close();
}
