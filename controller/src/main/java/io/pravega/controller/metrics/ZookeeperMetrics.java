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
package io.pravega.controller.metrics;

import static io.pravega.shared.MetricsNames.CONTROLLER_ZK_SESSION_EXPIRATION;

/**
 * Class to encapsulate the logic to report Controller metrics related to Zookeeper.
 */
public final class ZookeeperMetrics extends AbstractControllerMetrics {

    /**
     * This method reports a new session expiration event in a Controller instance.
     */
    public void reportZKSessionExpiration() {
        DYNAMIC_LOGGER.incCounterValue(CONTROLLER_ZK_SESSION_EXPIRATION, 1);
    }
}
