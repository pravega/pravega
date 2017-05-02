/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.client;

/**
 * Configuration of Zookeeper's Curator Framework client.
 */
public interface ZKClientConfig {
    /**
     * Fetches the ZK server connection string.
     *
     * @return The ZK server connection string.
     */
    String getConnectionString();

    /**
     * Fetches the ZK client namespace.
     *
     * @return The ZK client namespace.
     */
    String getNamespace();

    /**
     * Fetches the amount of sleep time before first retry.
     *
     * @return The amount of sleep time before first retry.
     */
    int getInitialSleepInterval();

    /**
     * Fetches the maximum nnumber of retries the client should make while attempting to connect to ZK servers.
     *
     * @return The maximum nnumber of retries the client should make while attempting to connect to ZK servers.
     */
    int getMaxRetries();
}
